package com.wajam.scn

import actors.Actor

import storage.ScnStorage
import com.yammer.metrics.scala.Instrumented
import com.wajam.commons.CurrentTime


/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T](name: String, storage: ScnStorage[T],
                       maxQueueSize: Long = 1000, messageExpirationMs: Long = 250)
  extends Actor with Instrumented with CurrentTime {

  case class SequenceActorMessage(callback: (List[T], Option[Exception]) => Unit,
                                  nb: Int,
                                  queuedTime: Long)

  private val expired = metrics.meter("message-expired", "message-expired", name)
  private val overflow = metrics.meter("message-queue-overflow", "message-queue-overflow", name)
  private val queueSize = metrics.gauge("message-queue-size", name) {
    mailboxSize
  }

  def next(cb: (List[T], Option[Exception]) => Unit, nb: Int) {
    val queueSize = mailboxSize
    if (queueSize < maxQueueSize) {
      this ! SequenceActorMessage(cb, nb, currentTime)
    } else {
      overflow.mark()
      val e = new Exception("Message queue overflow (%d > %d)".format(
        queueSize, maxQueueSize))
      cb(List[T](), Some(e))
    }
  }

  def act() {
    loop {
      react {
        case SequenceActorMessage(cb, nb, queuedTime) =>
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
