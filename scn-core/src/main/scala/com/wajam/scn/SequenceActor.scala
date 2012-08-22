package com.wajam.scn

import actors.Actor

import storage.ScnStorage

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T <% Comparable[T]](storage: ScnStorage[T]) extends Actor {
  private val MAX_BATCH_SIZE = 100
  private var lastGenerated = storage.head

  def next(cb: (List[T] => Unit), nb: Option[Int] = None) {
    this !(cb, nb)
  }

  def act() {
    loop {
      react {
        case (cb: (List[T] => Unit), optNb: Option[Int]) =>
          // Define the batch size
          val batchSize = optNb match {
            case Some(nb) =>
              List(nb, MAX_BATCH_SIZE).min
            case None =>
              MAX_BATCH_SIZE
          }

          var nextRange = storage.next(batchSize)
          // Next range first item must ALWAYS be greater than the last generated
          while (nextRange.head.compareTo(lastGenerated) != 1) {
           Thread.sleep(50)
            nextRange = storage.next(batchSize)
          }

          cb(nextRange)
          lastGenerated = nextRange.last
      }
    }
  }

}
