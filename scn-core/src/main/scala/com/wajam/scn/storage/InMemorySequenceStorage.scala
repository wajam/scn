package com.wajam.scn.storage

/**
 * Sequence storage that doesn't store sequence number, but keep it in memory.
 */
class InMemorySequenceStorage extends ScnStorage[Long] {

  private var lastSeq = SequenceRange(0, 1)

  /**
   * Get Head of the sequence
   * @return Head of the sequence (Next element to be returned)
   */
  def head: Long = lastSeq.from

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Long] = {
    lastSeq = SequenceRange(lastSeq.to, lastSeq.to + count)
    List.range(lastSeq.from, lastSeq.to)
  }

}
