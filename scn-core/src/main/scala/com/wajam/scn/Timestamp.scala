package com.wajam.scn

import storage.ScnTimestamp

/**
 * Describe a timestamp
 */
trait Timestamp extends Ordered[Timestamp] {
  def value: Long

  override def compare(t: Timestamp) = compareTo(t)
  override def compareTo(t: Timestamp): Int = {
    value.compareTo(t.value)
  }
}

object Timestamp {
  def apply(l: Long) = {
    ScnTimestamp(l)
  }

  def now = ScnTimestamp.now
  def MIN = ScnTimestamp.MIN
  def MAX = ScnTimestamp.MAX

  implicit def timestamp2long(ts: Timestamp) = ts.value
}
