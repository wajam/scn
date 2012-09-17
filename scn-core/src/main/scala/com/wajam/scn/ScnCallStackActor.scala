package com.wajam.scn

import java.util.{TimerTask, Timer}
import com.wajam.nrv.Logging
import actors.Actor
import scala.collection.mutable
import storage.{TimestampUtil, ScnTimestamp}

/**
 * Description
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
private class ScnSequenceCallStackActor[Long](scn: Scn, seqName: String, private val executionRateInMs: Int)
  extends ScnCallStackActor[Long](scn, seqName, executionRateInMs) {

  protected val stack = CountedScnCallStack[Long](new mutable.Stack[ScnCallback[Long]]())

  override protected def batch(cb: ScnCallback[Long]) {
    stack.push(cb)
  }

  override protected def execute() {
    val fullfillCnt = stack.count
    stack.count -= fullfillCnt
    if (fullfillCnt > 0) {
      scn.getNextSequence(seqName, (seq, optException) => {
        if (optException.isDefined) {
          stack.count += fullfillCnt
        } else {
          var range = SequenceRange(0, fullfillCnt)
          while (stack.hasMore && range.length >= stack.top.map(_.nb).getOrElse(0)) {
            val scnCb = stack.pop()
            // Fullfill the callback)
            scnCb.callback(seq.take(scnCb.nb).map(_.asInstanceOf[Long]), None)
            range = SequenceRange(scnCb.nb, seq.length)
          }
        }
      }, fullfillCnt)
    }
  }
}

private class ScnTimestampCallStackActor[Timestamp](scn: Scn, seqName: String, private val EXECUTION_RATE_IN_MS: Int)
  extends ScnCallStackActor[Timestamp](scn, seqName, EXECUTION_RATE_IN_MS) {

  protected val stack = CountedScnCallStack[Timestamp](new mutable.Stack[ScnCallback[Timestamp]]())
  private var lastAllocated: ScnTimestamp = TimestampUtil.MIN

  override protected def batch(cb: ScnCallback[Timestamp]) {
    stack.push(cb)
  }

  override protected def execute() {
    val fullfillCnt = stack.count
    stack.count -= fullfillCnt
    if (fullfillCnt > 0) {
      scn.getNextTimestamp(seqName, (seq, optException) => {
        if (optException.isDefined) {
          stack.count += fullfillCnt
        } else {
          var range = SequenceRange(0, fullfillCnt)
          while (stack.hasMore && range.length >= stack.top.map(_.nb).getOrElse(0)) {
            val scnCb = stack.pop()
            // Fullfill the callback
            val cSeq = seq.take(scnCb.nb)
            if (Timestamp(cSeq.head.toString.toLong).compareTo(Timestamp(lastAllocated)) >= 1) {
              scnCb.callback(cSeq.map(_.asInstanceOf[Timestamp]), None)
              range = SequenceRange(scnCb.nb, seq.length)
              lastAllocated = ScnTimestamp(cSeq.last)
            }
          }
        }
      }, fullfillCnt)
    }
  }
}

abstract private class ScnCallStackActor[T](scn: Scn, seqName: String, private val EXECUTION_RATE_IN_MS: Int)
  extends Actor with Logging {

  private val timer = new Timer
  protected val stack: CountedScnCallStack[T]

  protected def batch(cb: ScnCallback[T])

  protected def execute()

  def act() {
    loop {
      react {
        case Batched(cb: ScnCallback[T]) =>
          batch(cb)
        case Execute() =>
          execute()
        case _ => throw new UnsupportedOperationException
      }
    }
  }

  override def start(): Actor = {
    super.start()
    timer.scheduleAtFixedRate(new TimerTask {
      def run() {
        fullfill()
      }
    }, 0, EXECUTION_RATE_IN_MS)
    this
  }

  def fullfill() {
    this ! Execute()
  }
}
