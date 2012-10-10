package com.wajam.scn

import storage.ScnTimestamp

/**
 * Describe a timestamp
 */
trait Timestamp extends Comparable[Timestamp] {
  def value: Long

  def compareTo(t: Timestamp): Int = {
    value.compareTo(t.value)
  }
}

object Timestamp {
  def apply(l: Long) = {
    ScnTimestamp(l)
  }

  implicit def timestamp2long(ts: Timestamp) = ts.value
}
