package com.wajam.scn

import actors.Actor
import scala.collection.mutable.Map

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor(storage: SequenceStorage) extends Actor {
  private val BATCH_SIZE = 100
  private val sequences = Map[String, (Int, Int)]()

  object GetSequence

  def next(sequenceName: String, cb: (Int => Unit)) {
    this !(GetSequence, sequenceName, Some(cb))
  }

  def next(sequenceName: String): Int = {
    (this !?(GetSequence, sequenceName, None)).asInstanceOf[Int]
  }

  def act() {
    while (true) {
      receive {
        case (GetSequence, sequenceName: String, optCb: Option[(Int => Unit)]) =>
          val (sequence, to) = sequences.get(sequenceName) match {
            case Some((curSeq, curTo)) =>
              if (curSeq < curTo) {
                (curSeq, curTo)
              } else {
                storage.next(sequenceName, BATCH_SIZE)
              }
            case None =>
              storage.next(sequenceName, BATCH_SIZE)
          }

          sequences += (sequenceName -> (sequence+1, to))


          optCb match {
            case Some(cb) =>
              cb(sequence)
            case None =>
              sender ! sequence
          }
      }
    }
  }

}
