package com.wajam.scn

import actors.Actor

import storage.ScnStorage

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T <% Comparable[T]](storage: ScnStorage[T]) extends Actor {
  private val MAX_BATCH_SIZE = 100
  private var lastGenerated = storage.head

  def next(cb: (List[T] => Unit), nb: Int = MAX_BATCH_SIZE) {
    this !(cb, nb)
  }

  def act() {
    loop {
      react {
        case (cb: (List[T] => Unit), nb: Int) =>
          // Define the batch size
          val batchSize = math.min(nb, MAX_BATCH_SIZE)
          var nextRange = storage.next(batchSize)

          // Next range first item must ALWAYS be greater than the last generated
          while (nextRange.head.compareTo(lastGenerated) != 1) {
            Thread.sleep(50)
            nextRange = storage.next(batchSize)
          }

          lastGenerated = nextRange.last
          cb(nextRange)
      }
    }
  }

}
