package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.Future

@RunWith(classOf[JUnitRunner])
class TestZookeeperSequenceStorage extends FunSuite with BeforeAndAfter {
  val SEED = 1
  val NAME = "it_seq_test"
  val BATCH_SIZE = 100
  val zkServerAddress = "127.0.0.1/tests"
  var zkClient: ZookeeperClient = null
  var storage: ZookeeperSequenceStorage = null

  before {
    zkClient = new ZookeeperClient(zkServerAddress)
    try {
      zkClient.delete("/scn/sequence/%s".format(NAME))
    } catch {
      case e: Exception =>
    }
    storage = new ZookeeperSequenceStorage(zkClient, NAME, BATCH_SIZE, SEED)
  }

  after {
    storage = null
    zkClient.close()
    zkClient = null
  }

  test("initial value") {
    //make sure no value saved
    zkClient.delete("/scn/sequence/%s".format(NAME))

    val expectedInitValue = 10010
    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", 100, expectedInitValue)

    assert(expectedInitValue === storage.next(1)(0))
  }

  test("increment") {
    val seed = storage.next(1)
    val range = storage.next(10)

    assert(range.last - range.head === 9)
    assert((seed(0) + 1) === range.head)
    assert(range.size === 10, range.size)
  }

  test("other node increments in zookeeper") {

    storage.next(50) //this should reserve 1 to 100 for the test instance

    //simulate another node taking 1000 ids (100 - 1100)
    zkClient.incrementCounter("/scn/sequence/%s".format(NAME), 1000, 0)

    val ids = storage.next(100)

    assert(100 === ids.length)
    assert(51 === ids(0))
    assert(1150 === ids(99))
  }

  test("test seed greater than current zookeeper stored value should return seed") {
    //set counter to 4 in zookeeper
    zkClient.incrementCounter("/scn/sequence/%s".format(NAME), 4, 0)

    val seed = 1000

    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", 100, seed)

    val ids = storage.next(1)

    assert(seed === ids(0))
  }

  test("unicity of generated ids") {
    val unique = storage.next(10) ::: storage.next(20) ::: storage.next(30)

    assert(unique == unique.distinct, unique)
    assert(unique.size === 60, unique.size)
  }

  test("get unique id if exact batchsize is used for count") {
    val seq1 = storage.next(BATCH_SIZE)

    assert(1 === seq1(0))
    assert(BATCH_SIZE === seq1.length)
    assert(100 === seq1(99))

    val seq2 = storage.next(1)
    assert(1 === seq2.length)
    assert(101 === seq2(0))
  }

  test("concurent increment should never returns overlaping sequence") {

    // Setup storages
    val zkCLients = 1.to(5).map(_ => new ZookeeperClient(zkServerAddress)).toList
    val storages = zkCLients.map(new ZookeeperSequenceStorage(_, NAME, 50, SEED)).toList

    // Request sequences concurently (one thread per storage)
    val iterations = 25
    val countPerCall = 51
    val workers = storages.map(storage => Future.future({
      for (i <- 1 to iterations) yield storage.next(countPerCall)
    }))

    val all = for (worker <- workers) yield Future.blocking(worker)
    val allFlatten = all.flatten.flatten.toList
    allFlatten.size should be (workers.size * countPerCall * iterations)
    allFlatten.size should be (allFlatten.distinct.size)

    zkCLients.foreach(_.close())
  }

}
