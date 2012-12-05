package com.wajam.scn

/**
 * Describe a sequence range
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 *
 */
case class SequenceRange(from: Long, to: Long) {
  def length = to - from

  def toList = List.range[Long](from, to)
}

object SequenceRange {
  implicit def ranges2sequence(ranges: Seq[SequenceRange]): List[Long] = {
    ranges.foldLeft(List[Long]())(_ ::: _.toList)
  }

  implicit def sequence2ranges(sequence: Seq[Long]): List[SequenceRange] = {
    var ranges = List[SequenceRange]()
    if (sequence.size > 0) {
      var from = sequence.head
      var nextTo = from + 1
      for (i <- sequence.tail) {
        if (nextTo == i) {
          nextTo += 1
        } else {
          ranges = SequenceRange(from, nextTo) :: ranges
          from = i
          nextTo = from + 1
        }
      }
      ranges = SequenceRange(from, nextTo) :: ranges
    }
    ranges.reverse
  }
}
