package com.wajam.scn

import java.util.{TimerTask, Timer}
import com.wajam.nrv.Logging
import actors.Actor
import scala.collection.mutable
import storage.TimestampUtil
import com.wajam.nrv.tracing.Traced

/**
 * Description
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnSequenceCallQueueActor(scn: Scn, seqName: String, executionRateInMs: Int,
                                callbackExecutor: Option[CallbackExecutor[Long]] = None)
  extends ScnCallQueueActor[Long](scn, seqName, executionRateInMs, callbackExecutor) {

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

class ScnTimestampCallQueueActor(scn: Scn, seqName: String, execRateInMs: Int,
                                 callbackExecutor: Option[CallbackExecutor[Timestamp]] = None)
  extends ScnCallQueueActor[Timestamp](scn, seqName, execRateInMs, callbackExecutor) {

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

abstract class ScnCallQueueActor[T](scn: Scn, seqName: String, execRateInMs: Int,
                                    callbackExecutor: Option[CallbackExecutor[T]] = None)
  extends Actor with Logging with Traced {

  private val timer = new Timer

  protected val executor: CallbackExecutor[T] =
    callbackExecutor.getOrElse(new DefaultCallbackExecutor[T](scn).start().asInstanceOf[CallbackExecutor[T]])

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
        case Batched(cb: ScnCallback[T]) =>
          batch(cb)
        case Execute() =>
          execute()
        case ExecuteOnScnResponse(response: Seq[T], error: Option[Exception]) =>
          executeScnResponse(response, error)
        case other => println(other); throw new UnsupportedOperationException()
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

case class Batched[T](cb: ScnCallback[T])

case class Callback[T](cb: ScnCallback[T], response: Seq[T])

case class ExecuteOnScnResponse[T](response: Seq[T], error: Option[Exception])

case class Execute()

trait CallbackExecutor[T] {
  def executeCallback(cb: ScnCallback[T], response: Seq[T])
}

class DefaultCallbackExecutor[T](scn: Scn) extends Actor with CallbackExecutor[T] with Logging {
  def act() {
    loop {
      react {
        case Callback(cb: ScnCallback[T], res: Seq[T]) =>
          scn.tracer.trace(cb.context) {
            //execTimer.update(System.currentTimeMillis() - cb.startTime, TimeUnit.MILLISECONDS)
            cb.callback(res, None)
          }
        case o => log.warn("Invalide message to callback executor!")
      }
    }
  }

  def executeCallback(cb: ScnCallback[T], response: Seq[T]) {
    this ! Callback(cb, response)
  }
}







