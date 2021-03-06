package com.wajam.scn.storage

/**
 * Consistent storage system for sequence number
 */
trait ScnStorage[T] {

  /**
   * Get next sequence for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[T]

}
