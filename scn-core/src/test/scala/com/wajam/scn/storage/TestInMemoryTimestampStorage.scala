package com.wajam.scn.storage

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.ControlableCurrentTime
import com.wajam.scn.storage.ScnTimestamp._
import com.wajam.nrv.utils.timestamp.Timestamp

class TestInMemoryTimestampStorage extends FunSuite {
  test("increment") {
    val storage = new InMemoryTimestampStorage with ControlableCurrentTime
    val range: List[Timestamp] = storage.next(10)

    // Test the order of increment
    range should be(range.sorted)
    range.size should be(10)
  }

  test("unicity of generated ids") {
    val storage = new InMemoryTimestampStorage with ControlableCurrentTime
    val unique = ScnTimestamp.ranges2timestamps(storage.next(10) ::: storage.next(20) ::: storage.next(30))
    Thread.sleep(2000)
    val unique2 = ScnTimestamp.ranges2timestamps(unique ::: storage.next(20))

    unique2 should be(unique2.distinct)
    unique.size should be(60)
  }

  test("should be expected timestamp value") {
    val storage = new InMemoryTimestampStorage with ControlableCurrentTime
    val values: List[Timestamp] = storage.next(1)
    values(0) should be(ScnTimestamp(storage.currentTime, 0))
  }
}
