package com.wajam.scn.storage

import com.wajam.scn.SequenceRange
import com.wajam.commons.CurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Sequence storage that doesn't storage sequence timestamps, but keep it in memory.
 */
class InMemoryTimestampStorage(clock: CurrentTime = new CurrentTime {}) extends ScnStorage[SequenceRange] {

  private var lastTime = clock.currentTime
  private var lastTimeEndSeq: Int = 0

  /**
   * Get next sequence for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[SequenceRange] = {
    val now = clock.currentTime

    if (lastTime > now) {
      // Clock is late for some reason
      lastTimeEndSeq = 0
      Nil
    } else {
      // If already returned some timestamps in the same ms, continue the sequence for this ms. Never overflow the
      // sequence whether this is the same ms or a new ms.
      val startSeq = if (lastTime == now) lastTimeEndSeq else 0
      val endSeq = math.min(Timestamp.SeqPerMs, startSeq + count)
      val startTs = Timestamp(now, startSeq)
      lastTimeEndSeq = endSeq
      lastTime = now
      List(SequenceRange(startTs.value, startTs.value + endSeq - startSeq))
    }
  }
}
