package com.wajam.scn

trait Timestamp extends Comparable[Timestamp] {
  def value: Long

  def compareTo(t: Timestamp): Int = {
    value.compareTo(t.value)
  }
}

object Timestamp {
  implicit def timestamp2long(ts: Timestamp) = ts.value
}
