package com.wajam.scn

/**
 * Timestamp used to represent mutation time on storage
 */
case class Timestamp(var timevalue: Long, var seq: Long = 0) extends Comparable[Timestamp] {
  if (seq > Timestamp.MAX_SEQ_NO)
    throw new IndexOutOfBoundsException

  def this(timevalue: Long) = this(timevalue, 0)

  override def toString: String = value.toString

  override def equals(obj: Any): Boolean = obj match {
    case t: Timestamp => t.value == value
    case _ => false
  }

  def compareTo(t: Timestamp): Int = {
    if (timevalue.compareTo(t.timevalue) != 0) {
      timevalue.compareTo(t.timevalue)
    } else {
      if (seq.compareTo(t.seq) != 0) {
        seq.compareTo(t.seq)
      } else {
        0
      }
    }
  }

  def value: Long = timevalue * 10000 + seq
}

object Timestamp {
  val MIN_SEQ_NO = 0
  val MAX_SEQ_NO = 9999

  // *Warning* this should be removed since it might not be unique
  val now = Timestamp(System.currentTimeMillis())

  def MAX = Timestamp(Long.MaxValue, MAX_SEQ_NO)

  def MIN = Timestamp(0, MIN_SEQ_NO)

  implicit def timestamp2long(ts: Timestamp) = ts.value
}
