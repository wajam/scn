package com.wajam.scn

import actors.Actor
import collection.mutable
import java.util.{TimerTask, Timer}
import scala.Option
import com.wajam.nrv.Logging

/**
 * Description
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnClient(scn: Scn) {

  private val callStackActor = new ScnCallStackActor(scn)
  callStackActor.start()

  def getNextTimestamp(name: String, cb: (Any, Option[Exception]) => Unit, nb: Int) {
    callStackActor ! Next(name, ScnCallback(cb, nb), ScnCallbackType.timestamp)
  }

  def getNextSequence(name: String, cb: (Any, Option[Exception]) => Unit, nb: Int) {
    callStackActor ! Next(name, ScnCallback(cb, nb), ScnCallbackType.sequence)
  }

}

case class Next(name: String, cb: ScnCallback, seqType: ScnCallbackType.Value)

case class Fullfill()

private class ScnCallStackActor(scn: Scn) extends Actor with Logging {

  private val timer = new Timer
  private val TIMEOUT_CHECK_IN_MS = 10
  private val cbStacks = new mutable.HashMap[String, CountedScnCallStack]

  def act() {
    loop {
      react {
        case Next(name: String, cb: ScnCallback, cbType: ScnCallbackType.Value) =>
          cbStacks.getOrElse(name, {
            cbStacks.put(name, CountedScnCallStack(new mutable.Stack[ScnCallback](), cbType))
            cbStacks(name)
          }).push(cb)
        case Fullfill() =>
          cbStacks.foreach {
            case (name: String, stack: CountedScnCallStack) =>
              val fullfillCnt = stack.count
              if (fullfillCnt > 0) {
                stack.cbType match {
                  case ScnCallbackType.sequence =>
                    scn.getNextSequence(name, (seq, optException) => {
                      if (optException.isDefined) {
                        stack.count += fullfillCnt
                      } else {
                        var range = SequenceRange(0, seq.length)
                        while (stack.top != null && range.range >= stack.top.nb) {
                          val scnCb = stack.pop()
                          // Fullfill the callback
                          scnCb.callback(seq.slice(range.from.toInt, scnCb.nb), None)
                          range = SequenceRange(scnCb.nb, seq.length)
                        }
                      }
                    }, fullfillCnt)
                  case ScnCallbackType.timestamp =>
                    scn.getNextTimestamp(name, (seq, optException) => {
                      if (optException.isDefined) {
                        stack.count += fullfillCnt
                      } else {
                        var range = SequenceRange(0, seq.length)
                        while (stack.top != null && range.range >= stack.top.nb) {
                          val scnCb = stack.pop()
                          // Fullfill the callback
                          scnCb.callback(seq.slice(range.from.toInt, scnCb.nb), None)
                          range = SequenceRange(scnCb.nb, seq.length)
                        }
                      }
                    }, fullfillCnt)
                }
                stack.count -= fullfillCnt
              }
          }
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
    }, 0, TIMEOUT_CHECK_IN_MS)
    this
  }

  def fullfill() {
    this ! Fullfill()
  }
}

