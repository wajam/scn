package com.wajam.scn

import actors.Actor

import storage.ScnStorage
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.utils.CurrentTime

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T <% Comparable[T]](name: String, storage: ScnStorage[T],
                                        maxQueueSize: Long = 1000, messageExpirationMs: Long = 250)
  extends Actor with Instrumented with CurrentTime {

  private val expired = metrics.meter("message-expired", "message-expired", name)
  private val overflow = metrics.meter("message-queue-overflow", "message-queue-overflow", name)
  private val queueSize = metrics.gauge("message-queue-size", name) {
    mailboxSize
  }

  def next(cb: (List[T], Option[Exception]) => Unit, nb: Int) {
    if (mailboxSize > maxQueueSize) {
      // Overflowing messages are not dropped (at least not yet!)
      overflow.mark()
    }
    this !(cb, nb, currentTime)
  }

  def act() {
    loop {
      react {
        case (cb: ((List[T], Option[Exception]) => Unit), nb: Int, queuedTime: Long) =>
          try {
            if (queuedTime + messageExpirationMs > currentTime) {
              val nextRange = storage.next(nb)
              cb(nextRange, None)
            } else {
              expired.mark()
              val e = new Exception("Message expired and dropped (%d > %d)".format(
                currentTime - queuedTime, messageExpirationMs))
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
