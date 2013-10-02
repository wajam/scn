package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.scn.storage.ZookeeperTimestampStorage._
import com.wajam.scn.storage.ScnTimestamp._
import com.wajam.commons.{CurrentTime, ControlableCurrentTime}
import com.wajam.nrv.utils.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestZookeeperTimestampStorage extends FunSuite with BeforeAndAfter {
  val tsName = "it_ts_tests"
  val zkServerAddress = "127.0.0.1/tests"
  var zkClient: ZookeeperClient = null
  var storage: ZookeeperTimestampStorage = null

  before {
    zkClient = new ZookeeperClient(zkServerAddress)
    try {
      zkClient.delete(timestampPath(tsName))
    } catch {
      case e: Exception =>
    }
    storage = new ZookeeperTimestampStorage(zkClient, tsName, 5000, 1000)
  }

  after {
    storage = null
    zkClient.close()
    zkClient = null
  }

  test("increment") {
    val range: List[Timestamp] = storage.next(10)

    // Test the order of increment
    assert(range.sortWith((t1, t2) => t1.compareTo(t2) == -1) == range, range)
    assert(range.size == 10, range.size)
  }

  test("unicity of generated ids") {
    val unique = ScnTimestamp.ranges2timestamps(storage.next(10) ::: storage.next(20) ::: storage.next(30))
    Thread.sleep(2000)
    val unique2 = ScnTimestamp.ranges2timestamps(unique ::: storage.next(20))

    unique2 should be(unique2.distinct)
    unique.size should be(60)
  }

  test("should be expected timestamp value") {
    val storage = new ZookeeperTimestampStorage(zkClient, tsName, 5000, 1000) with ControlableCurrentTime
    val values: List[Timestamp] = storage.next(1)
    values(0) should be(ScnTimestamp(storage.currentTime, 0))
  }

  test("counter head position in zookeeper") {
    val head = storage.saveAheadTimestamp

    storage.next(1)
    // Wait 2 times to Save ahead time to make sure a new head is written
    Thread.sleep(storage.saveAheadInMs * 2)
    storage.next(1)

    // Head saved in Zookeeper must be smaller than now + save_ahead time since the request is done
    assert(head < ScnTimestamp(System.currentTimeMillis() + storage.saveAheadInMs, 0), head)
  }

  test("storage with drifting clock") {
    val saveAheadMillis = 5000
    val inTimeStorage = new ZookeeperTimestampStorage(zkClient, tsName, saveAheadMillis, 1000)
    val onTime: List[Timestamp] = inTimeStorage.next(1)

    val otherStorage = new ZookeeperTimestampStorage(zkClient, tsName, saveAheadMillis, 1000) with ControlableCurrentTime

    // Test other storage in time but before save ahead
    evaluating {
      otherStorage.next(1)
    } should produce [Exception]

    // Test other storage with one minute late clock
    otherStorage.currentTime -= (60 * 1000) // 1 minute late clock
    evaluating {
      otherStorage.next(1)
    } should produce [Exception]

    // Test other storage again after advancing one minute + save ahead
    otherStorage.currentTime += (60 * 1000 + saveAheadMillis)
    val backOnTime: List[Timestamp] = otherStorage.next(1)

    onTime(0) should be < (backOnTime(0))
  }

  test("concurent instances overlaps") {
    val saveAheadMillis = 5000
    val renewalMillis = 1000
    val initialTime = new CurrentTime{}.currentTime

    val zk1 = new ZookeeperClient(zkServerAddress)
    val storage1 = new ZookeeperTimestampStorage(zk1, tsName, saveAheadMillis, renewalMillis) with ControlableCurrentTime

    val zk2 = new ZookeeperClient(zkServerAddress)
    val storage2 = new ZookeeperTimestampStorage(zk2, tsName, saveAheadMillis, renewalMillis) with ControlableCurrentTime

    // First instance initial timestamp
    storage1.currentTime = initialTime
    storage1.next(1)

    // Second instance not able to generate timestamp until save ahead expires
    storage2.currentTime = initialTime
    evaluating {
      storage2.next(1)
    } should produce [Exception]

    // Second instance able to generate timestamp at save ahead expiration
    storage2.currentTime += saveAheadMillis
    storage2.next(1)

    // First instance not able to generate timestamp anymore at save ahead expiration because second instance is now in charge
    storage1.currentTime = storage2.currentTime
    evaluating {
      storage1.next(1)
    } should produce [Exception]

    // Second instance should update save ahead expiration at renewal time before real expiration. Because of this
    // the first instance is not be able to generate a timestamp at the original save ahead expiration time
    storage2.currentTime += saveAheadMillis - renewalMillis
    storage2.next(1)

    storage1.currentTime += saveAheadMillis
    evaluating {
      storage1.next(1)
    } should produce [Exception]

    // First instance able to generate timestamp again after save ahead expiration
    storage1.currentTime += saveAheadMillis
    storage1.next(1)

    // Now the second instance fail as expected...
    storage2.currentTime = storage1.currentTime
    evaluating {
      storage2.next(1)
    } should produce [Exception]

    zk1.close()
    zk2.close()
  }

}
