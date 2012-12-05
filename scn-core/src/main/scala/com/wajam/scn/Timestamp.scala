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
  def apply(l: Long): Timestamp = {
    ScnTimestamp(l)
  }

  def now = ScnTimestamp.now
  def MIN = ScnTimestamp.MIN
  def MAX = ScnTimestamp.MAX

  implicit def timestamp2long(ts: Timestamp) = ts.value

  implicit def ranges2timestamps(ranges: Seq[SequenceRange]): List[Timestamp] = {
    SequenceRange.ranges2sequence(ranges).map(Timestamp(_))
  }

  implicit def timestamps2ranges(sequence: Seq[Timestamp]): List[SequenceRange] = {
    SequenceRange.sequence2ranges(sequence.map(_.value))
  }
}
