package com.wajam.scn.storage

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.commons.ControlableCurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.scn.SequenceRange
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.lang.IllegalArgumentException
import java.util.concurrent.Executors
import language.postfixOps

@RunWith(classOf[JUnitRunner])
class TestInMemoryTimestampStorage extends FunSuite {
  test("should generate sequence of timestamps") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L
    val storage = new InMemoryTimestampStorage(clock)
    val range: List[Timestamp] = storage.next(5)

    range should be(List(Timestamp(10L, 0), Timestamp(10L, 1), Timestamp(10L, 2), Timestamp(10L, 3), Timestamp(10L, 4)))
    range.size should be(5)
  }

  test("should continue sequence in same ms") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L

    val storage = new InMemoryTimestampStorage(clock)
    val r1: List[Timestamp] = storage.next(2)
    val r2: List[Timestamp] = storage.next(1)
    val r3: List[Timestamp] = storage.next(3)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1)))
    r2 should be(List(Timestamp(10L, 2)))
    r3 should be(List(Timestamp(10L, 3), Timestamp(10L, 4), Timestamp(10L, 5)))
  }

  test("should reset sequence when starting a new ms") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L

    val storage = new InMemoryTimestampStorage(clock)
    val r1: List[Timestamp] = storage.next(2)

    clock.currentTime = 20L
    val r2: List[Timestamp] = storage.next(1)
    val r3: List[Timestamp] = storage.next(3)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1)))
    r2 should be(List(Timestamp(20L, 0)))
    r3 should be(List(Timestamp(20L, 1), Timestamp(20L, 2), Timestamp(20L, 3)))
  }

  test("should return an empty range if clock is late") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L

    val storage = new InMemoryTimestampStorage(clock)
    clock.currentTime -= 1
    storage.next(1) should be(Nil)
  }

  test("should not overflow when continuing in the same ms") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L

    val storage = new InMemoryTimestampStorage(clock)
    val r1: List[Timestamp] = storage.next(5)
    val r2: List[Timestamp] = storage.next(Timestamp.SeqPerMs)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1), Timestamp(10L, 2), Timestamp(10L, 3), Timestamp(10L, 4)))
    r2.last should be(Timestamp(10L, Timestamp.SeqPerMs - 1))
    r2.size should be(Timestamp.SeqPerMs - 5)
  }

  test("should not overflow when starting in a new ms") {
    val clock = new ControlableCurrentTime {}
    clock.currentTime = 10L

    val storage = new InMemoryTimestampStorage(clock)
    val r1: List[Timestamp] = storage.next(5)

    clock.currentTime = 20L
    val r2: List[Timestamp] = storage.next(Timestamp.SeqPerMs)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1), Timestamp(10L, 2), Timestamp(10L, 3), Timestamp(10L, 4)))
    r2.head should be(Timestamp(20L, 0))
    r2.last should be(Timestamp(20L, Timestamp.SeqPerMs - 1))
    r2.size should be(Timestamp.SeqPerMs)
  }
}
