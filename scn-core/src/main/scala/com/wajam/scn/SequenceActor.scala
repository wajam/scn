package com.wajam.scn

import actors.Actor

import storage.ScnStorage
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.CurrentTime

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T <% Comparable[T]](name: String, storage: ScnStorage[T], expirationInMs: Long = 250)
  extends Actor with Instrumented with CurrentTime {

  private val dropped = metrics.meter("message-dropped-old", "message-dropped-old", name)
  private val queueSize = metrics.gauge("message-queue-size", name) {
    mailboxSize
  }

  def next(cb: (List[T], Option[Exception]) => Unit, nb: Int) {
    this !(cb, nb, currentTime)
  }

  def act() {
    loop {
      react {
        case (cb: ((List[T], Option[Exception]) => Unit), nb: Int, queuedTime: Long) =>
          try {
            if (queuedTime + expirationInMs > currentTime) {
              val nextRange = storage.next(nb)
              cb(nextRange, None)
            } else {
              dropped.mark()
              val e = new Exception("Message dropped because it is too old (%d > %d)".format(
                currentTime - queuedTime, expirationInMs))
              cb(List[T](), Some(e))
            }
          } catch {
            case e: Exception =>
              cb(List[T](), Some(e))
          }
      }
    }
  }

}
