package com.wajam.scn.storage

import com.wajam.scn.Timestamp
import math.Integral

/**
 * Sequence storage that doesn't storage sequence timestamps, but keep it in memory.
 */
class InMemoryTimestampStorage extends ScnStorage[Timestamp] {

  private var lastTime = System.currentTimeMillis()
  private var lastSeq = SequenceRange(0, 1)

  /**
   * Get Head of the sequence
   * @return Head of the sequence (Next element to be returned)
   */
  def head = Timestamp(System.currentTimeMillis(), lastSeq.from)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Timestamp] = {
    val reqTime = System.currentTimeMillis()
    lastSeq = if (lastTime == reqTime) {
      SequenceRange(lastSeq.to, lastSeq.to + count)
    } else {
      SequenceRange(1, count + 1)
    }

    lastTime = reqTime
    List.range(Timestamp(reqTime, lastSeq.from).value, Timestamp(reqTime, lastSeq.to).value).map(l => l:Timestamp)
  }
}
