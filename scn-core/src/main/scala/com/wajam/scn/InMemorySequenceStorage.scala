package com.wajam.scn

import scala.collection.mutable.Map

/**
 * Sequence storage that doesn't storage sequence number, but keep it in memory.
 */
class InMemorySequenceStorage extends SequenceStorage {
  private var sequenceCount = Map[String, Int]()

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param name Name of the sequence
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(name: String, count: Int): (Int, Int) = {
    val (from, to) = sequenceCount.get(name) match {
      case Some(current) =>
        (current, count - 1)
      case None =>
        (0, count - 1)
    }

    sequenceCount += (name -> (count + 1))
    (from, to)
  }
}
