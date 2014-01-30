package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._
import com.wajam.scn.storage.ZookeeperTimestampStorage._
import com.wajam.commons.{CurrentTime, ControlableCurrentTime}
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestZookeeperTimestampStorage extends FunSuite with BeforeAndAfter {
  val sequenceName = "it_ts_tests"
  val zkServerAddress = "127.0.0.1/tests"
  val saveAheadInMs = 5000
  val clock = new ControlableCurrentTime {}
  var zkClient: ZookeeperClient = null
  var storage: ZookeeperTimestampStorage = null

  before {
    zkClient = new ZookeeperClient(zkServerAddress)
    try {
      zkClient.delete(timestampPath(sequenceName))
    } catch {
      case e: Exception =>
    }
    clock.currentTime = 10L
    storage = new ZookeeperTimestampStorage(zkClient, sequenceName, saveAheadInMs, 1000, clock)
  }

  after {
    storage = null
    zkClient.close()
    zkClient = null
  }

  test("should generate sequence of timestamps") {
    val now = clock.currentTime
    val range: List[Timestamp] = storage.next(5)

    range should be(List(Timestamp(now, 0), Timestamp(now, 1), Timestamp(now, 2), Timestamp(now, 3), Timestamp(now, 4)))
    range.size should be(5)
  }

  test("save ahead position in zookeeper") {
    storage.next(1)
    val expectedInitialSaveAhead = Timestamp(clock.currentTime + saveAheadInMs, 0)
    storage.saveAheadTimestamp should be(expectedInitialSaveAhead)

    // ZK save ahead does not change if time advance less than reserved time
    clock.currentTime += saveAheadInMs / 2
    storage.next(1)
    storage.saveAheadTimestamp should be(expectedInitialSaveAhead)

    // ZK save ahead change if time advance past reserved time
    clock.currentTime += saveAheadInMs * 2
    storage.next(1)
    storage.saveAheadTimestamp should be(Timestamp(clock.currentTime + saveAheadInMs, 0))
  }

  test("storage with drifting clock") {
    val clock = new ControlableCurrentTime {}
    val inTimeStorage = new ZookeeperTimestampStorage(zkClient, sequenceName, saveAheadInMs, 1000, clock)
    val originalRange: List[Timestamp] = inTimeStorage.next(1)

    val otherClock = new ControlableCurrentTime {}
    val otherStorage = new ZookeeperTimestampStorage(zkClient, sequenceName, saveAheadInMs, 1000, otherClock)

    // Test other storage in time but before save ahead
    evaluating {
      otherStorage.next(1)
    } should produce [Exception]

    // Test other storage with one minute late clock
    otherClock.currentTime -= (60 * 1000) // 1 minute late clock
    evaluating {
      otherStorage.next(1)
    } should produce [Exception]

    // Test other storage again after advancing one minute + save ahead
    otherClock.currentTime += (60 * 1000 + saveAheadInMs)
    val laterRange: List[Timestamp] = otherStorage.next(1)

    originalRange(0) should be < laterRange(0)
  }

  test("concurrent instances overlaps") {
    val renewalMillis = 1000
    val initialTime = new CurrentTime{}.currentTime

    val zk1 = new ZookeeperClient(zkServerAddress)
    val clock1 = new ControlableCurrentTime {}
    val storage1 = new ZookeeperTimestampStorage(zk1, sequenceName, saveAheadInMs, renewalMillis, clock1)

    val zk2 = new ZookeeperClient(zkServerAddress)
    val clock2 = new ControlableCurrentTime {}
    val storage2 = new ZookeeperTimestampStorage(zk2, sequenceName, saveAheadInMs, renewalMillis, clock2)

    // First instance initial timestamp
    clock1.currentTime = initialTime
    storage1.next(1)

    // Second instance not able to generate timestamp until save ahead expires
    clock2.currentTime = initialTime
    evaluating {
      storage2.next(1)
    } should produce [Exception]

    // Second instance able to generate timestamp at save ahead expiration
    clock2.currentTime += saveAheadInMs
    storage2.next(1)

    // First instance not able to generate timestamp anymore at save ahead expiration because second instance is now in charge
    clock1.currentTime = clock2.currentTime
    evaluating {
      storage1.next(1)
    } should produce [Exception]

    // Second instance should update save ahead expiration at renewal time before real expiration. Because of this
    // the first instance is not be able to generate a timestamp at the original save ahead expiration time
    clock2.currentTime += saveAheadInMs - renewalMillis
    storage2.next(1)

    clock1.currentTime += saveAheadInMs
    evaluating {
      storage1.next(1)
    } should produce [Exception]

    // First instance able to generate timestamp again after save ahead expiration
    clock1.currentTime += saveAheadInMs
    storage1.next(1)

    // Now the second instance fail as expected...
    clock2.currentTime = clock1.currentTime
    evaluating {
      storage2.next(1)
    } should produce [Exception]

    zk1.close()
    zk2.close()
  }

  test("should continue sequence in same ms") {
    clock.currentTime = 10L
    val r1: List[Timestamp] = storage.next(2)
    val r2: List[Timestamp] = storage.next(1)
    val r3: List[Timestamp] = storage.next(3)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1)))
    r2 should be(List(Timestamp(10L, 2)))
    r3 should be(List(Timestamp(10L, 3), Timestamp(10L, 4), Timestamp(10L, 5)))
  }

  test("should reset sequence when starting a new ms") {
    clock.currentTime = 10L
    val r1: List[Timestamp] = storage.next(2)

    clock.currentTime = 20L
    val r2: List[Timestamp] = storage.next(1)
    val r3: List[Timestamp] = storage.next(3)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1)))
    r2 should be(List(Timestamp(20L, 0)))
    r3 should be(List(Timestamp(20L, 1), Timestamp(20L, 2), Timestamp(20L, 3)))
  }

  test("should return an empty range if clock is late") {
    clock.currentTime = 10L
    storage.next(1) should not be Nil
    clock.currentTime -= 1
    storage.next(1) should be(Nil)
  }

  test("should not overflow when continuing in the same ms") {
    clock.currentTime = 10L
    val r1: List[Timestamp] = storage.next(5)
    val r2: List[Timestamp] = storage.next(Timestamp.SeqPerMs)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1), Timestamp(10L, 2), Timestamp(10L, 3), Timestamp(10L, 4)))
    r2.last should be(Timestamp(10L, Timestamp.SeqPerMs - 1))
    r2.size should be(Timestamp.SeqPerMs - 5)
  }

  test("should not overflow when starting in a new ms") {
    clock.currentTime = 10L
    val r1: List[Timestamp] = storage.next(5)

    clock.currentTime = 20L
    val r2: List[Timestamp] = storage.next(Timestamp.SeqPerMs)

    r1 should be(List(Timestamp(10L, 0), Timestamp(10L, 1), Timestamp(10L, 2), Timestamp(10L, 3), Timestamp(10L, 4)))
    r2.head should be(Timestamp(20L, 0))
    r2.last should be(Timestamp(20L, Timestamp.SeqPerMs - 1))
    r2.size should be(Timestamp.SeqPerMs)
  }
}
