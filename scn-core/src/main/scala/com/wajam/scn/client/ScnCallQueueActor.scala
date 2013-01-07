package com.wajam.scn.client

import com.wajam.nrv.tracing.Traced
import com.wajam.scn.{SequenceRange, Timestamp, Scn}
import java.util.concurrent.TimeUnit
import actors.Actor
import com.wajam.nrv.{UnavailableException, TimeoutException, Logging}
import com.yammer.metrics.scala.Instrumented
import java.util.{TimerTask, Timer}
import collection.mutable
import util.Random
import com.wajam.nrv.service.Resolver
import math._
import scala.Left
import com.wajam.scn.storage.ScnTimestamp

/**
 * Actor that batches scn calls to get sequence numbers. It periodically call scn to get sequence numbers and then
 * assign those sequence numbers to the caller in order.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 *
 */
class ScnSequenceCallQueueActor(scn: Scn, seqName: String, executionRateInMs: Int, timeoutInMs: Int,
                                executors: List[CallbackExecutor[Long]])
  extends ScnCallQueueActor[SequenceRange, Long](scn, seqName, "sequence", executionRateInMs, timeoutInMs, executors) with Instrumented {

  def this(scn: Scn, seqName: String, executionRateInMs: Int, timeoutInMs: Int, callbackExecutor: CallbackExecutor[Long]) =
    this(scn, seqName, executionRateInMs, timeoutInMs, List(callbackExecutor))

  private val responseError = metrics.meter("response-error", "response-error", metricName)
  private val reponseSeqOutOfOrder = metrics.meter("response-sequence-out-of-order", "response-sequence-out-of-order", metricName)

  private var lastSequenceRange: SequenceRange = SequenceRange(0, 0)

  protected def scnMethod = {
    scn.getNextSequence _
  }

  override protected[scn] def executeScnResponse(response: Seq[SequenceRange], optException: Option[Exception]) {
    if (optException.isDefined) {
      responseError.mark()
      debug("Exception while fetching sequence numbers. {}", optException.get)
    } else {
      // Convert ranges into a sequence, droping out of order ranges in the process
      var sequenceNumbers = response.foldLeft(List[Long]())((sequences, range) => {
        if (range.from >= lastSequenceRange.to) {
          lastSequenceRange = range
          sequences ::: range.toList
        } else {
          reponseSeqOutOfOrder.mark()
          debug("Received out of order sequence, discarding.")
          sequences
        }
      })
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
 *
 */
class ScnTimestampCallQueueActor(scn: Scn, seqName: String, execRateInMs: Int, timeoutInMs: Int,
                                 executors: List[CallbackExecutor[Timestamp]])
  extends ScnCallQueueActor[SequenceRange, Timestamp](scn, seqName, "timestamps", execRateInMs, timeoutInMs, executors) {

  def this(scn: Scn, seqName: String, execRateInMs: Int, timeoutInMs: Int, callbackExecutor: CallbackExecutor[Timestamp]) =
    this(scn, seqName, execRateInMs, timeoutInMs, List(callbackExecutor))

  private val responseError = metrics.meter("response-error", "response-error", metricName)
  private val responseOutdated = metrics.meter("response-outdated", "response-outdated", metricName)

  private var lastAllocated: Timestamp = ScnTimestamp.MIN

  protected def scnMethod = {
    scn.getNextTimestamp _
  }

  override protected[scn] def executeScnResponse(response: Seq[SequenceRange], optException: Option[Exception]) {
    if (optException.isDefined) {
      responseError.mark()
      debug("Exception while fetching timestamps. {}", optException.get)
    } else if (Timestamp(response.head.from).compareTo(lastAllocated) < 1) {
      responseOutdated.mark()
      debug("Received outdated timestamps, discarding.")
    } else {
      var timestamps = ScnTimestamp.ranges2timestamps(response)
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
abstract class ScnCallQueueActor[ST, CT](scn: Scn, seqName: String, seqType: String, execRateInMs: Int, timeoutInMs: Int,
                                    val executors: List[CallbackExecutor[CT]])
  extends Actor with Logging with Instrumented {

  protected val metricName = seqName.replace(".", "_")

  private val unavailableServer = metrics.meter("server-unavailable", "server-unavailable", metricName)
  private val responseTimeout = metrics.meter("response-timeout", "response-timeout", metricName)
  private val executeRateTimer = metrics.timer("execute-rate", metricName)
  private val queueSizeGauge = metrics.gauge[Int]("queue-size", metricName)({
    queue.count
  })
  private val mailboxSizeGauge = metrics.gauge[Int]("mailbox-size", metricName)({
    mailboxSize
  })

  private val timer = new Timer

  private var lastExecuteTime = 0L

  protected val queue: CountedScnCallQueue[CT] = CountedScnCallQueue[CT](new mutable.Queue[ScnCallback[CT]]())

  protected def scnMethod: (String, (Seq[ST], Option[Exception]) => Unit, Int) => Unit

  protected[scn] def batch(cb: ScnCallback[CT]) {
    require(cb.nb > 0)
    queue.enqueue(cb)
  }

  protected[scn] def execute() {
    try {
      val currentTime = System.currentTimeMillis()

      // Monitor execution rate
      if (lastExecuteTime > 0) {
        executeRateTimer.update(currentTime - lastExecuteTime, TimeUnit.MILLISECONDS)
      }
      lastExecuteTime = currentTime

      // Timeout expired queued callbacks
      while (queue.hasMore && (currentTime - queue.front.get.startTime) > timeoutInMs) {
        val callback = queue.dequeue()
        responseTimeout.mark()
        val elapsed = currentTime - callback.startTime
        executeCallback(callback, Left(new TimeoutException("Timed out while waiting for SCN response.", Some(elapsed))))
      }

      // Call SCN for queued callbacks
      if (queue.count > 0) {
        scnMethod(seqName, (seq: Seq[ST], optException: Option[Exception]) => {
          this ! ExecuteOnScnResponse(if (seq == null) Seq() else seq, optException)
        }, queue.count)
      }
    } finally {
      timer.schedule(new TimerTask {
        def run() {
          fullfill()
        }
      }, execRateInMs)
    }
  }

  protected[scn] def executeScnResponse(response: Seq[ST], error: Option[Exception])

  protected[scn] def executeCallback(cb: ScnCallback[CT], response: Either[Exception, Seq[CT]]) {
    val token = if (cb.token >= 0) cb.token else Random.nextLong() % Resolver.MAX_TOKEN
    executors(abs((token % executors.size).toInt)).executeCallback(cb, response)
  }

  def act() {
    loop {
      react {
        case toBatch: Batched[CT] =>
          try {
            batch(toBatch.cb)
          } catch {
            case e: Exception => warn("Got an error on SCN batch {}", e)
          }
        case Execute() =>
          try {
            execute()
          } catch {
            case e: UnavailableException => {
              unavailableServer.mark()
              debug("Got an unvailable error on SCN call {}", e)
            }
            case e: Exception => warn("Got an error on SCN call {}", e)
          }
        case ExecuteOnScnResponse(response: Seq[ST], error: Option[Exception]) =>
          try {
            executeScnResponse(response, error)
          } catch {
            case e: Exception => warn("Got an error on SCN response {}", e)
          }
        case _ => throw new UnsupportedOperationException()
      }
    }
  }

  override def start(): Actor = {
    super.start()
    fullfill()
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
case class Batched[CT](cb: ScnCallback[CT])

// used to periodically execute call to Scn
case class Execute()

// used to execute the Scn callback from the caller
case class Callback[CT](cb: ScnCallback[CT], response: Either[Exception, Seq[CT]])

// used to execute the Scn response and dispatch to the callers
case class ExecuteOnScnResponse[ST](response: Seq[ST], error: Option[Exception])


/**
 * Callback executor interface. This interface executes the callback from the caller.
 */
trait CallbackExecutor[CT] {

  /**
   * Execute the callback
   * @param cb the callback to execute
   * @param response the response associated to the callback
   */
  def executeCallback(cb: ScnCallback[CT], response: Either[Exception, Seq[CT]])
}

/**
 * Default implementation that reset the trace context and call the callback.
 * The callback is executed in an actor message loop.
 */
class ClientCallbackExecutor[CT](name: String, scn: Scn) extends Actor with CallbackExecutor[CT] with Traced with Logging {
  val scnResponseTimer = tracedTimer("callback-time")

  def queueSize = mailboxSize

  def act() {
    loop {
      react {
        case callback: Callback[CT] =>
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

  def executeCallback(cb: ScnCallback[CT], response: Either[Exception, Seq[CT]]) {
    this ! Callback(cb, response)
  }
}
