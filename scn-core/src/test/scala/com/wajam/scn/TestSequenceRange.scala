package com.wajam.scn

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSequenceRange extends FunSuite {
  test("should return proper lenght") {
    val from = 1
    val count = 4
    SequenceRange(from, from + count).length should be(count)
  }

  test("should return proper list") {
    val from = 1
    val count = 4
    SequenceRange(from, from + count).toList should be(List(1L, 2L, 3L, 4L))
  }

  test("should convert ranges to sequence") {
    val ranges = List(SequenceRange(1, 3), SequenceRange(7, 8), SequenceRange(111, 114))
    val expected = List(1L, 2L, 7L, 111L, 112L, 113L)
    SequenceRange.ranges2sequence(ranges) should be(expected)

    // test implicit conversion
    import SequenceRange._
    val actual: List[Long] = ranges
    actual should be(expected)
  }

  test("should convert empty range to empty sequence") {
    val ranges = List[SequenceRange]()
    val expected = List[Long]()
    SequenceRange.ranges2sequence(ranges) should be(expected)
  }

  test("should convert sequence to ranges") {
    val sequence = List(1L, 2L, 7L, 111L, 112L, 113L, 1974L)
    val expected = List(SequenceRange(1, 3), SequenceRange(7, 8), SequenceRange(111, 114), SequenceRange(1974, 1975))
    SequenceRange.sequence2ranges(sequence) should be(expected)

    // test implicit conversion
    import SequenceRange._
    val actual: List[SequenceRange] = sequence
    actual should be(expected)
  }

  test("should convert empty sequence to empty range") {
    val sequence = List[Long]()
    val expected = List[SequenceRange]()
    SequenceRange.sequence2ranges(sequence) should be(expected)
  }
}
