package com.wajam.scn

import java.util.{TimerTask, Timer}
import com.wajam.nrv.Logging
import actors.Actor
import scala.collection.mutable
import storage.TimestampUtil
import com.wajam.nrv.tracing.Traced
import java.util.concurrent.TimeUnit

/**
 * Actor that batches scn calls to get sequence numbers. It periodically call scn to get sequence numbers and then
 * assign those sequence numbers to the caller in order.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnSequenceCallQueueActor(scn: Scn, seqName: String, executionRateInMs: Int,
                                callbackExecutor: Option[CallbackExecutor[Long]] = None)
  extends ScnCallQueueActor[Long](scn, seqName, "sequence", executionRateInMs, callbackExecutor) {

  protected def scnMethod = {
    scn.getNextSequence _
  }

  override protected[scn] def executeScnResponse(response: Seq[Long], optException: Option[Exception]) {
    if (optException.isDefined) {
      log.warn("Exception while fetching sequence numbers.", optException.get)
    } else {
      var sequenceNumbers = response
      while (queue.hasMore && sequenceNumbers.size >= queue.front.get.nb) {
        val scnCb = queue.dequeue()
        executor.executeCallback(scnCb, sequenceNumbers.take(scnCb.nb))
        sequenceNumbers = sequenceNumbers.drop(scnCb.nb)
      }
    }
  }
}

/**
 * Actor that batches scn calls to get timestamps. It periodically call scn to get timestamps and then
 * assign those timestamp numbers to the caller in order.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnTimestampCallQueueActor(scn: Scn, seqName: String, execRateInMs: Int,
                                 callbackExecutor: Option[CallbackExecutor[Timestamp]] = None)
  extends ScnCallQueueActor[Timestamp](scn, seqName, "timestamps", execRateInMs, callbackExecutor) {

  private var lastAllocated: Timestamp = TimestampUtil.MIN

  protected def scnMethod = {
    scn.getNextTimestamp _
  }

  override protected[scn] def executeScnResponse(response: Seq[Timestamp], optException: Option[Exception]) {
    if (optException.isDefined) {
      log.warn("Exception while fetching timestamps.", optException.get)
    } else if (Timestamp(response.head.toString.toLong).compareTo(lastAllocated) < 1) {
      log.info("Received outdated timestamps, discarding.")
    } else {
      var timestamps = response
      while (queue.hasMore && timestamps.size >= queue.front.get.nb) {
        val scnCb = queue.dequeue()
        val allocatedTimeStamps = timestamps.take(scnCb.nb)
        executor.executeCallback(scnCb, allocatedTimeStamps)
        lastAllocated = allocatedTimeStamps.last
        timestamps = timestamps.drop(scnCb.nb)
      }
    }
  }
}

/**
 * Base class for call queue actors.
 */
abstract class ScnCallQueueActor[T](scn: Scn, seqName: String, seqType: String, execRateInMs: Int,
                                    callbackExecutor: Option[CallbackExecutor[T]] = None)
  extends Actor with Logging with Traced {

  private val timer = new Timer

  protected val executor: CallbackExecutor[T] =
    callbackExecutor.getOrElse(
      new DefaultCallbackExecutor[T](seqType+"-"+seqName, scn).start().asInstanceOf[CallbackExecutor[T]])

  protected val queue: CountedScnCallQueue[T] = CountedScnCallQueue[T](new mutable.Queue[ScnCallback[T]]())

  protected def scnMethod: (String, (Seq[T], Option[Exception]) => Unit, Int) => Unit

  protected[scn] def batch(cb: ScnCallback[T]) {
    require(cb.nb > 0)
    queue.enqueue(cb)
  }

  protected[scn] def execute() {
    if (queue.count > 0) {
      scnMethod(seqName, (seq: Seq[T], optException: Option[Exception]) => {
        this ! ExecuteOnScnResponse(if(seq == null) Seq() else seq, optException)
      }, queue.count)
    }
  }

  protected[scn] def executeScnResponse(response: Seq[T], error: Option[Exception])

  def act() {
    loop {
      react {
        case toBatch: Batched[T] =>
          batch(toBatch.cb)
        case Execute() =>
          execute()
        case ExecuteOnScnResponse(response: Seq[T], error: Option[Exception]) =>
          executeScnResponse(response, error)
        case _ => throw new UnsupportedOperationException()
      }
    }
  }

  override def start(): Actor = {
    super.start()
    timer.scheduleAtFixedRate(new TimerTask {
      def run() {
        fullfill()
      }
    }, 0, execRateInMs)
    this
  }

  def fullfill() {
    this ! Execute()
  }
}

/**
 * Call queue actors message classes
 */
// used to batch a scn call
case class Batched[T](cb: ScnCallback[T])

// used to periodically execute call to Scn
case class Execute()

// used to execute the Scn callback from the caller
case class Callback[T](cb: ScnCallback[T], response: Seq[T])

// used to execute the Scn response and dispatch to the callers
case class ExecuteOnScnResponse[T](response: Seq[T], error: Option[Exception])


/**
 * Callback executor interface. This interface executes the callback from the caller.
 */
trait CallbackExecutor[T] {

  /**
   * Execute the callback
   * @param cb the callback to execute
   * @param response the response associated to the callback
   */
  def executeCallback(cb: ScnCallback[T], response: Seq[T])
}

/**
 * Default implementation that reset the trace context and call the callback.
 * The callback is executed in an actor message loop.
 */
class DefaultCallbackExecutor[T](name: String, scn: Scn) extends Actor with CallbackExecutor[T] with Traced with Logging {
  val scnResponseTimer = tracedTimer(name + "-callback")
  def act() {
    loop {
      react {
        case callback: Callback[T] =>
          scnResponseTimer.update(System.currentTimeMillis() - callback.cb.startTime, TimeUnit.MILLISECONDS)
          scn.tracer.trace(callback.cb.context) {
            try {
              callback.cb.callback(callback.response, None)
            }
            catch {
              case ex: Exception => {
                log.warn("Caught exception while executing the SCN callback." +
                  " The callback code should catch exceptions and reply instead of throwing an exception.", ex)
              }
            }
          }
        case _ => log.warn("Invalide message to callback executor!")
      }
    }
  }

  def executeCallback(cb: ScnCallback[T], response: Seq[T]) {
    this ! Callback(cb, response)
  }
}







