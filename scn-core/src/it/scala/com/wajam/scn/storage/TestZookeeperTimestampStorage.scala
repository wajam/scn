package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestZookeeperTimestampStorage extends FunSuite with BeforeAndAfter {
  val TS_NAME = "it_ts_tests"
  val zkClient = new ZookeeperClient("127.0.0.1")
  val storage = new ZookeeperTimestampStorage(zkClient, TS_NAME)

  test("increment") {
    val range = storage.next(10)

    // Test the order of increment
    assert(range.sortWith((t1, t2) => t1.compareTo(t2) == -1) == range, range)
    assert(range.size == 10, range.size)
  }

  test("unicity of generated ids") {
    val unique = storage.next(10) ::: storage.next(20) ::: storage.next(30)
    Thread.sleep(2000)
    val unique2 = unique ::: storage.next(20)

    assert(unique2 == unique2.distinct, unique)
    assert(unique.size === 60, unique.size)
  }

  test("counter head position in zookeeper") {
    val head = storage.head

    storage.next(1)
    // Wait 2 times to Save ahead time to make sure a new head is written
    Thread.sleep(storage.SAVE_AHEAD_MS * 2)
    storage.next(1)

    // Head saved in Zookeeper must be smaller than now + save_ahead time since the request is done
    assert(head < ScnTimestamp(System.currentTimeMillis() + storage.SAVE_AHEAD_MS, 0), head)
  }

  test("storage with drifting clock") {
    val inTimeStorage = new ZookeeperTimestampStorage(zkClient, TS_NAME + 1)
    val onTime = inTimeStorage.next(1)

    val driftedStorage = new ZookeeperTimestampStorage(zkClient, TS_NAME + 1) with CurrentTime {
      override def getCurrentTime = System.currentTimeMillis() - (10 * 1000) // 1 minute late clock
    }

    intercept[Exception] {
      driftedStorage.next(1)
    }
  }

}
