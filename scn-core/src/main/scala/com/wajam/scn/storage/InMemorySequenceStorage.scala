package com.wajam.scn.storage

import scala.collection.mutable.Map
import scala.Some

/**
 * Sequence storage that doesn't store sequence number, but keep it in memory.
 */
class InMemorySequenceStorage extends ScnStorage[Int] {

  private var lastSeq = SequenceRange(0, 0)

  /**
   * Get Head of the sequence
   * @return Head of the sequence (Next element to be returned)
   */
  def head: Int = lastSeq.from

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Int] = {
    lastSeq = SequenceRange(lastSeq.to, lastSeq.to + count)
    List.range(lastSeq.from, lastSeq.to)
  }

}
