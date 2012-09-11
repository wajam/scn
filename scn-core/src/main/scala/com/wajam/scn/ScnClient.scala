package com.wajam.scn

import actors.Actor
import collection.mutable

/**
 * Description
 */
class ScnClient(scn: Scn) {

  private val seqBuffer = new ScnClientBuffer[Int](scn)

  def getNextSequence(name: String, cb: (List[Int], Option[Exception]) => Unit, nb: Int) {
    seqBuffer ! Get[Int](name, nb, (seq, optEx) => {

    })
  }
}

case class Get[T](name: String, n: Int, cb: (List[T], Option[Exception]) => Unit)

class ScnClientBuffer[T](scn: Scn) extends Actor {

  val BATCH_SIZE = 100

  private val buffer: mutable.ListBuffer[T] = mutable.ListBuffer.empty[T]

  def act() {
    loop {
      react {
        case Get(name: String, n: Int, cb: ((List[T], Option[Exception]) => Unit)) =>
          if (buffer.length < n) {
            scn.nextSequence.call(params = Map("name" -> name, "nb" -> math.max(BATCH_SIZE, n)), onReply = (respMsg, optException) => {
              if (optException.isEmpty)
                buffer ++= respMsg.parameters("sequence").asInstanceOf[List[T]]
              else
                cb(Nil, Some(new Exception("Error fetching sequence from SCN.")))
            })
          }

          cb(buffer.take(n).toList, None)
          buffer --= buffer.take(n)
      }
    }
  }

}
