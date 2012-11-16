package com.wajam.scn

import java.util.{TimerTask, Timer}
import com.wajam.nrv.{TimeoutException, Logging}
import actors.Actor
import scala.collection.mutable
import storage.TimestampUtil
import com.wajam.nrv.tracing.Traced
import java.util.concurrent.TimeUnit
import util.Random
import com.wajam.nrv.service.Resolver
import com.yammer.metrics.scala.Instrumented

/**
 * Actor that batches scn calls to get sequence numbers. It periodically call scn to get sequence numbers and then
 * assign those sequence numbers to the caller in order.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnSequenceCallQueueActor(scn: Scn, seqName: String, executionRateInMs: Int, timeoutInMs: Int,
                                executors: List[CallbackExecutor[Long]])
  extends ScnCallQueueActor[Long](scn, seqName, "sequence", executionRateInMs, timeoutInMs, executors) with Instrumented {

  def this(scn: Scn, seqName: String, executionRateInMs: Int, timeoutInMs: Int, callbackExecutor: CallbackExecutor[Long]) =
    this(scn, seqName, executionRateInMs, timeoutInMs, List(callbackExecutor))

  private val responseError = metrics.meter("scn-client-response-error", "scn-client-response-error", seqName)

  protected def scnMethod = {
    scn.getNextSequence _
  }

  override protected[scn] def executeScnResponse(response: Seq[Long], optException: Option[Exception]) {
    if (optException.isDefined) {
      responseError.mark()
      debug("Exception while fetching sequence numbers. {}", optException.get)
    } else {
      var sequenceNumbers = response
      while (queue.hasMore && sequenceNumbers.size >= queue.front.get.nb) {
        val scnCb = queue.dequeue()
        executeCallback(scnCb, Right(sequenceNumbers.take(scnCb.nb)))
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
class ScnTimestampCallQueueActor(scn: Scn, seqName: String, execRateInMs: Int, timeoutInMs: Int,
                                 executors: List[CallbackExecutor[Timestamp]])
  extends ScnCallQueueActor[Timestamp](scn, seqName, "timestamps", execRateInMs, timeoutInMs, executors) {

  def this(scn: Scn, seqName: String, execRateInMs: Int, timeoutInMs: Int, callbackExecutor: CallbackExecutor[Timestamp]) =
    this(scn, seqName, execRateInMs, timeoutInMs, List(callbackExecutor))

  private val responseError = metrics.meter("scn-client-response-error", "scn-client-response-error", seqName)
  private val responseOutdated = metrics.meter("scn-client-response-outdated", "scn-client-response-outdated", seqName)

  private var lastAllocated: Timestamp = TimestampUtil.MIN

  protected def scnMethod = {
    scn.getNextTimestamp _
  }

  override protected[scn] def executeScnResponse(response: Seq[Timestamp], optException: Option[Exception]) {
    if (optException.isDefined) {
      responseError.mark()
      debug("Exception while fetching timestamps. {}", optException.get)
    } else if (Timestamp(response.head.toString.toLong).compareTo(lastAllocated) < 1) {
      responseOutdated.mark()
      info("Received outdated timestamps, discarding.")
    } else {
      var timestamps = response
      while (queue.hasMore && timestamps.size >= queue.front.get.nb) {
        val scnCb = queue.dequeue()
        val allocatedTimeStamps = timestamps.take(scnCb.nb)
        executeCallback(scnCb, Right(allocatedTimeStamps))
        lastAllocated = allocatedTimeStamps.last
        timestamps = timestamps.drop(scnCb.nb)
      }
    }
  }
}

/**
 * Base class for call queue actors.
 */
abstract class ScnCallQueueActor[T](scn: Scn, seqName: String, seqType: String, execRateInMs: Int, timeoutInMs: Int,
                                    val executors: List[CallbackExecutor[T]])
  extends Actor with Logging with Instrumented {

  private val timer = new Timer

  protected val queue: CountedScnCallQueue[T] = CountedScnCallQueue[T](new mutable.Queue[ScnCallback[T]]())

  protected def scnMethod: (String, (Seq[T], Option[Exception]) => Unit, Int) => Unit

  protected[scn] def batch(cb: ScnCallback[T]) {
    require(cb.nb > 0)
    queue.enqueue(cb)
  }

  protected[scn] def execute() {
    val currentTime = System.currentTimeMillis()
    while (queue.hasMore && (currentTime - queue.front.get.startTime) > timeoutInMs) {
      val callback = queue.dequeue()
      executeCallback(callback, Left(new TimeoutException("Timed out while waiting for SCN response.")))
    }
    if (queue.count > 0) {
      scnMethod(seqName, (seq: Seq[T], optException: Option[Exception]) => {
        this ! ExecuteOnScnResponse(if (seq == null) Seq() else seq, optException)
      }, queue.count)
    }
  }

  protected[scn] def executeScnResponse(response: Seq[T], error: Option[Exception])

  protected[scn] def executeCallback(cb: ScnCallback[T], response: Either[Exception, Seq[T]]) {
    val token = if (cb.token >= 0) cb.token else Random.nextLong() % Resolver.MAX_TOKEN
    executors((token % executors.size).toInt).executeCallback(cb, response)
  }

  def act() {
    loop {
      react {
        case toBatch: Batched[T] =>
          batch(toBatch.cb)
        case Execute() =>
          try {
            execute()
          } catch {
            case e: Exception => error("Got an error in SCN call queue actor {}", e)
          }
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
case class Callback[T](cb: ScnCallback[T], response: Either[Exception, Seq[T]])

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
  def executeCallback(cb: ScnCallback[T], response: Either[Exception, Seq[T]])
}

/**
 * Default implementation that reset the trace context and call the callback.
 * The callback is executed in an actor message loop.
 */
class ClientCallbackExecutor[T](name: String, scn: Scn) extends Actor with CallbackExecutor[T] with Traced with Logging {
  val scnResponseTimer = tracedTimer("scn-client-callback-time", name)

  def queueSize = mailboxSize

  def act() {
    loop {
      react {
        case callback: Callback[T] =>
          scnResponseTimer.update(System.currentTimeMillis() - callback.cb.startTime, TimeUnit.MILLISECONDS)
          scn.tracer.trace(callback.cb.context) {
            try {
              callback.response match {
                case Left(e) => callback.cb.callback(Seq(), Some(e))
                case Right(r) => callback.cb.callback(r, None)
              }
            }
            catch {
              case ex: Exception => {
                warn("Caught exception while executing the SCN callback." +
                  " The callback code should catch exceptions and reply instead of throwing an exception.", ex)
              }
            }
          }
        case _ => warn("Invalide message to callback executor!")
      }
    }
  }

  def executeCallback(cb: ScnCallback[T], response: Either[Exception, Seq[T]]) {
    this ! Callback(cb, response)
  }
}







