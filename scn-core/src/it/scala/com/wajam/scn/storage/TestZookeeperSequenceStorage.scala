package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.{ControlableCurrentTime, Future}
import com.wajam.scn.storage.ZookeeperSequenceStorage._
import com.wajam.scn.SequenceRange


@RunWith(classOf[JUnitRunner])
class TestZookeeperSequenceStorage extends FunSuite with BeforeAndAfter {
  val seqSeed = 1
  val seqName = "it_seq_test"
  val batchSize = 100
  val zkServerAddress = "127.0.0.1/tests"

  var zkClient: ZookeeperClient = null
  var storage: ZookeeperSequenceStorage = null

  before {
    zkClient = new ZookeeperClient(zkServerAddress)
    try {
      zkClient.delete(sequencePath(seqName))
    } catch {
      case e: Exception =>
    }
    storage = new ZookeeperSequenceStorage(zkClient, seqName, batchSize, seqSeed)
  }

  after {
    storage = null
    zkClient.close()
    zkClient = null
  }

  test("initial value") {
    //make sure no value saved
    zkClient.delete(sequencePath(seqName))

    val expectedInitValue = 10010
    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", 100, expectedInitValue)

    assert(expectedInitValue === SequenceRange.ranges2sequence(storage.next(1))(0))
  }

  test("increment") {
    val seed = SequenceRange.ranges2sequence(storage.next(1))
    val range = SequenceRange.ranges2sequence(storage.next(10))

    assert(range.last - range.head === 9)
    assert((seed(0) + 1) === range.head)
    assert(range.size === 10, range.size)
  }

  test("other node increments in zookeeper") {

    storage.next(50) //this should reserve 1 to 100 for the test instance

    //simulate another node taking 1000 ids (100 - 1100)
    zkClient.incrementCounter(sequencePath(seqName), 1000, 0)

    val ids = SequenceRange.ranges2sequence(storage.next(100))

    assert(100 === ids.length)
    assert(51 === ids(0))
    assert(1150 === ids(99))
  }

  test("test seed greater than current zookeeper stored value should return seed") {
    //set counter to 4 in zookeeper
    zkClient.incrementCounter(sequencePath(seqName), 4, 0)

    val seed = 1000

    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", 100, seed)

    val ids = SequenceRange.ranges2sequence(storage.next(1))

    assert(seed === ids(0))
  }

  test("unicity of generated ids") {
    val unique = SequenceRange.ranges2sequence(storage.next(10) ::: storage.next(20) ::: storage.next(30))

    assert(unique == unique.distinct, unique)
    assert(unique.size === 60, unique.size)
  }

  test("get unique id if exact batchsize is used for count") {
    val seq1 = SequenceRange.ranges2sequence(storage.next(batchSize))

    assert(1 === seq1(0))
    assert(batchSize === seq1.length)
    assert(100 === seq1(99))

    val seq2 = SequenceRange.ranges2sequence(storage.next(1))
    assert(1 === seq2.length)
    assert(101 === seq2(0))
  }

  test("concurent increment should never returns overlaping sequence") {

    // Setup storages
    val zkCLients = 1.to(5).map(_ => new ZookeeperClient(zkServerAddress)).toList
    val storages = zkCLients.map(new ZookeeperSequenceStorage(_, seqName, 50, seqSeed)).toList

    // Request sequences concurently (one thread per storage)
    val iterations = 25
    val countPerCall = 51
    val workers = storages.map(storage => Future.future({
      for (i <- 1 to iterations) yield SequenceRange.ranges2sequence(storage.next(countPerCall))
    }))

    val all = for (worker <- workers) yield Future.blocking(worker)
    val allFlatten = all.flatten.flatten.toList
    allFlatten.size should be (workers.size * countPerCall * iterations)
    allFlatten.size should be (allFlatten.distinct.size)

    zkCLients.foreach(_.close())
  }

  test("dynamicaly increase save ahead") {

    val storage = new ZookeeperSequenceStorage(zkClient, seqName, batchSize, seqSeed) with ControlableCurrentTime
    def getZkValue = zkClient.getLong(sequencePath(seqName))

    // Requested count is much less than min save ahead, should increment using min save ahead
    val initialZkValue = getZkValue
    storage.next(1)
    val zkValue1 = getZkValue
    zkValue1 should be (initialZkValue + batchSize)
    storage.next(1)
    getZkValue should be (zkValue1)

    // Request more than save ahead in the last 10 seconds (2 buckets of 5 secs), should increment > min save ahead
    var seq1 = SequenceRange.ranges2sequence(storage.next(batchSize))
    val zkValue2 = getZkValue
    zkValue2 should be > (zkValue1 + batchSize)
    storage.next((zkValue2 - seq1.max - 2).toInt)
    getZkValue should be (zkValue2)

    // Skip more than 5 seconds in time with a small count to flush the current bucket
    storage.currentTime += 70000
    val seq2 = SequenceRange.ranges2sequence(storage.next(1))
    getZkValue should be (zkValue2)
    seq2.max should be (getZkValue - 1)

    // Skip again in time to flush the previous bucket. Requested count is much less than min save ahead and
    // should increment using min save ahead again
    storage.currentTime += 70000
    storage.next(1)
    getZkValue should be (zkValue2 + batchSize)
  }
}
