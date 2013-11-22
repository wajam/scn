package com.wajam.scn.storage

import com.wajam.scn.SequenceRange
import com.wajam.commons.CurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Sequence storage that doesn't storage sequence timestamps, but keep it in memory.
 */
class InMemoryTimestampStorage(protected val clock: CurrentTime = new CurrentTime {}) extends TimestampStorage {

  protected var lastTime = clock.currentTime

  /**
   * Get next sequence for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   */
  def next(count: Int): List[SequenceRange] = next(count, clock.currentTime)
}
