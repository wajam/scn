package com.wajam.scn

/**
 * Timestamp used to represent mutation time on storage
 */
class Timestamp(private var timevalue: Long, private var seq: Long) extends Serializable with Comparable[Timestamp] {

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
  def time: Long = timevalue

}

object Timestamp {
  val MIN_SEQ_NO = 0
  val MAX_SEQ_NO = 9999

  def apply(timevalue: Long) = new Timestamp(timevalue, 0)
  def apply(timevalue:Long, seq: Long) = new Timestamp(timevalue, seq)

  def now = new Timestamp(System.currentTimeMillis(), MIN_SEQ_NO)

  val MAX = new Timestamp(Long.MaxValue, MAX_SEQ_NO)

  val MIN = new Timestamp(0, MIN_SEQ_NO)

  implicit def long2timestamp(value: Long) = Timestamp(value / 10000, value % 10000)

  implicit def timestamp2long(ts: Timestamp) = ts.value
}
