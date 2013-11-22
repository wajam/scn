package com.wajam.scn

import scala.language.implicitConversions
import com.wajam.nrv.utils.timestamp.Timestamp

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

  implicit def ranges2timestamps(ranges: Seq[SequenceRange]): List[Timestamp] = {
    SequenceRange.ranges2sequence(ranges).map(Timestamp(_))
  }

  implicit def timestamps2ranges(sequence: Seq[Timestamp]): List[SequenceRange] = {
    SequenceRange.sequence2ranges(sequence.map(_.value))
  }
}
