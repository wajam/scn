package com.wajam.scn.storage

import com.wajam.scn.Timestamp

/**
 * Timestamp used to represent mutation time on storage
 */
private[scn] case class ScnTimestamp(value: Long) extends Timestamp {

  override def toString: String = value.toString

}

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

  def now = ScnTimestamp(System.currentTimeMillis(), ScnTimestamp.MIN_SEQ_NO)
  val MIN = ScnTimestamp(0, ScnTimestamp.MIN_SEQ_NO)
  val MAX = ScnTimestamp(Long.MaxValue)
}
