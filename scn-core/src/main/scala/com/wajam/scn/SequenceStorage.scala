package com.wajam.scn

/**
 * Consistent storage system for sequence number
 */
trait SequenceStorage {
  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param name Name of the sequence
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(name: String, count: Int): (Int, Int)
}
