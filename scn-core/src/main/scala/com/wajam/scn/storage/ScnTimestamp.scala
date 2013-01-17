package com.wajam.scn.storage

import com.wajam.scn.SequenceRange
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Timestamp used to represent mutation time on storage
 */
private[scn] case class ScnTimestamp(value: Long) extends Timestamp

object ScnTimestamp {
  val MIN_SEQ_NO = 0
  val MAX_SEQ_NO = 9999

  def apply(ts: Timestamp): ScnTimestamp = {
    new ScnTimestamp(ts.value)
  }

  def apply(timevalue: Long, seq: Long): ScnTimestamp = {
    if (seq > ScnTimestamp.MAX_SEQ_NO)
      throw new IndexOutOfBoundsException

    new ScnTimestamp(timevalue * 10000 + seq)
  }

  def now: Timestamp = ScnTimestamp(System.currentTimeMillis(), ScnTimestamp.MIN_SEQ_NO)
  val MIN: Timestamp = ScnTimestamp(0, ScnTimestamp.MIN_SEQ_NO)
  val MAX: Timestamp = ScnTimestamp(Long.MaxValue)

  implicit def ranges2timestamps(ranges: Seq[SequenceRange]): List[Timestamp] = {
    SequenceRange.ranges2sequence(ranges).map(ScnTimestamp(_))
  }

  implicit def timestamps2ranges(sequence: Seq[Timestamp]): List[SequenceRange] = {
    SequenceRange.sequence2ranges(sequence.map(_.value))
  }
}
