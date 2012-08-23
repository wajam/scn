package com.wajam.scn.storage

import com.wajam.scn.Timestamp

/**
 * Sequence storage that doesn't storage sequence timestamps, but keep it in memory.
 */
class InMemoryTimestampStorage extends ScnStorage[Timestamp] with CurrentTime {

  private var lastTime = getCurrentTime
  private var lastSeq = SequenceRange(0, 1)

  /**
   * Get Head of the sequence
   * @return Head of the sequence (Next element to be returned)
   */
  def head = Timestamp(getCurrentTime, lastSeq.from)

  /**
   * Get next sequence for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Timestamp] = {
    val reqTime = getCurrentTime
    lastSeq = if (lastTime == reqTime) {
      SequenceRange(lastSeq.to, lastSeq.to + count)
    } else {
      SequenceRange(1, count + 1)
    }

    lastTime = reqTime
    List.range(lastSeq.from, lastSeq.to).map(l => Timestamp(lastTime, l))
  }
}
