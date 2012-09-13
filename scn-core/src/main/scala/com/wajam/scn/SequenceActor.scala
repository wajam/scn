package com.wajam.scn

import actors.Actor

import storage.ScnStorage

/**
 * Actor that receives sequence requests and returns sequence numbers
 */
class SequenceActor[T <% Comparable[T]](storage: ScnStorage[T]) extends Actor {
  private var lastGenerated = storage.head

  def next(cb: (List[T] => Unit), nb: Int) {
    this !(cb, nb)
  }

  def act() {
    loop {
      react {
        case (cb: (List[T] => Unit), nb: Int) =>
          val nextRange = storage.next(nb)

          lastGenerated = nextRange.last
          cb(nextRange)
      }
    }
  }

}
