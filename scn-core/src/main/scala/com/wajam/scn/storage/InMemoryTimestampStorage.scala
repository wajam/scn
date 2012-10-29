package com.wajam.scn.storage

import com.wajam.scn.{SequenceRange, Timestamp}

/**
 * Sequence storage that doesn't storage sequence timestamps, but keep it in memory.
 */
class InMemoryTimestampStorage extends ScnStorage[Timestamp] with CurrentTime {

  private var lastTime = currentTime
  private var lastSeq = SequenceRange(0, 1)

  /**
   * Get next sequence for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[ScnTimestamp] = {
    var reqTime = currentTime

    while (lastSeq.length != count) {
      lastSeq = if (lastTime == reqTime) {
        SequenceRange(lastSeq.to, lastSeq.to + count)
      } else if (lastTime < reqTime) {
        SequenceRange(1, count + 1)
      } else {
        reqTime = currentTime
        SequenceRange(0, 0)
      }
    }

    lastTime = reqTime
    List.range(lastSeq.from, lastSeq.to).map(l => ScnTimestamp(lastTime, l))
  }
}
