package com.wajam.scn

import actors.Actor
import scala.collection.mutable.Map

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor(storage: SequenceStorage) extends Actor {
  private val MIN_BATCH_SIZE = 100
  private val sequences = Map[String, (Int, Int)]()

  object GetSequence

  def next(sequenceName: String, cb: (Int => Unit), nb: Option[Int] = None) {
    this !(GetSequence, sequenceName, cb, nb)
  }

  def act() {
    loop {
      react {
        case (GetSequence, sequenceName: String, cb: (Int => Unit), optNb: Option[Int]) =>
          val batchSize = optNb match {
            case Some(nb) =>
              List(nb, MIN_BATCH_SIZE).min
            case None =>
              MIN_BATCH_SIZE
          }

          val (sequence, to) = sequences.get(sequenceName) match {
            case Some((curSeq, curTo)) =>
              if (curSeq <= curTo) {
                (curSeq, curTo)
              } else {
                storage.next(sequenceName, batchSize)
              }
            case None =>
              storage.next(sequenceName, batchSize)
          }

          sequences += (sequenceName -> (sequence + 1, to))

          cb(sequence)
      }
    }
  }

}
