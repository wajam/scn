package com.wajam.scn.storage

import com.wajam.scn.SequenceRange
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.commons.CurrentTime

trait TimestampStorage extends ScnStorage[SequenceRange] {
  protected def clock: CurrentTime

  protected var lastTime = clock.currentTime
  private var lastTimeEndSeq: Int = 0

  def next(count: Int, now: Long): List[SequenceRange] = {
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
