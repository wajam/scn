package com.wajam.scn.storage

import com.wajam.scn.Timestamp

/**
 * Timestamp used to represent mutation time on storage
 */
private[storage] case class ScnTimestamp(timevalue: Long, seq: Long = 0) extends Timestamp {
  val MIN_SEQ_NO = 0
  val MAX_SEQ_NO = 9999

  if (seq > MAX_SEQ_NO)
    throw new IndexOutOfBoundsException

  override def toString: String = value.toString

  val value: Long = timevalue * 10000 + seq
}

@Deprecated()
object TimestampUtil {
  val MIN_SEQ_NO = 0
  val MAX_SEQ_NO = 9999

  val now = ScnTimestamp(System.currentTimeMillis())

  def MAX = ScnTimestamp(Long.MaxValue, MAX_SEQ_NO)

  def MIN = ScnTimestamp(0, MIN_SEQ_NO)
}
