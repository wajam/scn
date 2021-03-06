package com.wajam.scn.storage

import com.wajam.scn.SequenceRange

/**
 * Sequence storage that doesn't store sequence number, but keep it in memory.
 */
class InMemorySequenceStorage extends ScnStorage[SequenceRange] {

  private var lastSeq = SequenceRange(0, 1)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[SequenceRange] = {
    lastSeq = SequenceRange(lastSeq.to, lastSeq.to + count)
    List(lastSeq)
  }

}
