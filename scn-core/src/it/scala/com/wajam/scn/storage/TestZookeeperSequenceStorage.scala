package com.wajam.scn.storage {

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.{ControlableCurrentTime, Future}
import com.wajam.scn.storage.ZookeeperSequenceStorage._
import com.wajam.scn.SequenceRange
import com.wajam.nrv.cluster.{Node, LocalNode, Cluster}
import com.wajam.nrv.service.{MemberStatus, ServiceMember, Service}
import com.wajam.nrv.NotifiableStaticClusterManager


@RunWith(classOf[JUnitRunner])
class TestZookeeperSequenceStorage extends FunSuite with BeforeAndAfter {
  val seed = 1
  val seqName = "it_seq_test"
  val saveAheadSize = 100
  val zkServerAddress = "127.0.0.1/tests"

  var zkClient: ZookeeperClient = null
  var storage: ZookeeperSequenceStorage = null

  var clusterManager: NotifiableStaticClusterManager = null
  var cluster: Cluster = null
  var service: Service = null
  var localMember: ServiceMember = null
  var remoteMember: ServiceMember = null
  var otherMember: ServiceMember = null

  before {
    // Setup test cluster
    clusterManager = new NotifiableStaticClusterManager
    cluster = new Cluster(new LocalNode(Map("nrv" -> 49999)), clusterManager)
    service = new Service("test")
    localMember = new ServiceMember(0, cluster.localNode)
    remoteMember = new ServiceMember(1, new Node("wajam.com", Map("nrv" -> 49999)))
    service.addMember(localMember)
    service.addMember(remoteMember)
    cluster.registerService(service)

    val otherService = new Service("other")
    otherMember = new ServiceMember(1, cluster.localNode)
    cluster.registerService(otherService)
    otherService.addMember(otherMember)

    cluster.start()

    // Setup storage
    zkClient = new ZookeeperClient(zkServerAddress)
    try {
      zkClient.delete(sequencePath(seqName))
    } catch {
      case e: Exception =>
    }
    storage = new ZookeeperSequenceStorage(zkClient, seqName, saveAheadSize, service, seed)
  }

  after {
    cluster.stop()
    clusterManager = null
    cluster = null
    service = null
    localMember = null
    remoteMember = null
    otherMember = null

    storage = null
    zkClient.close()
    zkClient = null
  }

  test("initial value") {
    //make sure no value saved
    zkClient.delete(sequencePath(seqName))

    val expectedInitValue = 10010
    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", 100, service, expectedInitValue)

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
    zkClient.set(sequencePath(seqName), 4)
    zkClient.getLong(sequencePath(seqName)) should be(4)

    val seed = 1000
    val saveAhead = 100

    storage = new ZookeeperSequenceStorage(
      new ZookeeperClient(zkServerAddress), "it_seq_test", saveAhead, service, seed)

    val ids = SequenceRange.ranges2sequence(storage.next(1))
    ids(0) should be(seed)
    zkClient.getLong(sequencePath(seqName)) should be(seed + saveAhead)
  }

  test("unicity of generated ids") {
    val unique = SequenceRange.ranges2sequence(storage.next(10) ::: storage.next(20) ::: storage.next(30))

    assert(unique == unique.distinct, unique)
    assert(unique.size === 60, unique.size)
  }

  test("get unique id if exact batchsize is used for count") {
    val seq1 = SequenceRange.ranges2sequence(storage.next(saveAheadSize))

    assert(1 === seq1(0))
    assert(saveAheadSize === seq1.length)
    assert(100 === seq1(99))

    val seq2 = SequenceRange.ranges2sequence(storage.next(1))
    assert(1 === seq2.length)
    assert(101 === seq2(0))
  }

  test("concurent increment should never returns overlaping sequence") {

    // Setup storages
    val zkCLients = 1.to(5).map(_ => new ZookeeperClient(zkServerAddress)).toList
    val storages = zkCLients.map(new ZookeeperSequenceStorage(_, seqName, 50, service, seed)).toList

    // Request sequences concurently (one thread per storage)
    val iterations = 25
    val countPerCall = 51
    val workers = storages.map(storage => Future.future({
      for (i <- 1 to iterations) yield SequenceRange.ranges2sequence(storage.next(countPerCall))
    }))

    val all = for (worker <- workers) yield Future.blocking(worker)
    val allFlatten = all.flatten.flatten.toList
    allFlatten.size should be(workers.size * countPerCall * iterations)
    allFlatten.size should be(allFlatten.distinct.size)

    zkCLients.foreach(_.close())
  }

  test("dynamicaly increase save ahead") {

    val storage = new ZookeeperSequenceStorage(zkClient, seqName, saveAheadSize, service, seed) with ControlableCurrentTime
    def getZkValue = zkClient.getLong(sequencePath(seqName))

    // Requested count is much less than min save ahead, should increment using min save ahead
    val initialZkValue = getZkValue
    storage.next(1)
    val zkValue1 = getZkValue
    zkValue1 should be(initialZkValue + saveAheadSize)
    storage.next(1)
    getZkValue should be(zkValue1)

    // Request more than save ahead in the last 10 seconds (2 buckets of 5 secs), should increment > min save ahead
    var seq1 = SequenceRange.ranges2sequence(storage.next(saveAheadSize))
    val zkValue2 = getZkValue
    zkValue2 should be > (zkValue1 + saveAheadSize)
    storage.next((zkValue2 - seq1.max - 2).toInt)
    getZkValue should be(zkValue2)

    // Skip more than 5 seconds in time with a small count to flush the current bucket
    storage.currentTime += 70000
    val seq2 = SequenceRange.ranges2sequence(storage.next(1))
    getZkValue should be(zkValue2)
    seq2.max should be(getZkValue - 1)

    // Skip again in time to flush the previous bucket. Requested count is much less than min save ahead and
    // should increment using min save ahead again
    storage.currentTime += 70000
    storage.next(1)
    getZkValue should be(zkValue2 + saveAheadSize)
  }

  test("should reset current sequence on Scn service member status change") {

    // Request a sequence. Should reserve a range in zookeeper
    val seq1 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId1 = zkClient.getLong(sequencePath(seqName))
    actualReservedId1 should be(seed + saveAheadSize)
    seq1(0) should be(seed)

    // Requesting a new sequence again should not affect reserved range in zookeeper
    val seq2 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId2 = zkClient.getLong(sequencePath(seqName))
    actualReservedId2 should be(actualReservedId1)
    seq2(0) should be(seed + 1)

    // Change status of a service member after changing the reserved range in zookeeper.
    // Should reserve a new range in zookeeper.
    zkClient.set(sequencePath(seqName), actualReservedId2 + 1)
    clusterManager.setMemberStatus(localMember, MemberStatus.Down)
    val seq3 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId3 = zkClient.getLong(sequencePath(seqName))
    actualReservedId3 should be(actualReservedId2 + 1 + saveAheadSize)
    seq3(0) should be(actualReservedId3 - saveAheadSize)

    // Change status again without modifying the reserved range in zookeeper.
    // Should not reserve a new range.
    clusterManager.setMemberStatus(localMember, MemberStatus.Up)
    val seq4 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId4 = zkClient.getLong(sequencePath(seqName))
    actualReservedId4 should be(actualReservedId3)
    seq4(0) should be(seq3(0) + 1)

    // Change status of an unrelated service member after changing the reserved range in zookeeper.
    // Does not reserve a new range in zookeeper since we dont check zookeeper for unrelated service member status change
    zkClient.set(sequencePath(seqName), actualReservedId4 + 1)
    clusterManager.setMemberStatus(otherMember, MemberStatus.Down)
    val seq5 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId5 = zkClient.getLong(sequencePath(seqName))
    actualReservedId5 should be(actualReservedId4 + 1)
    seq5(0) should be(seq4(0) + 1)

    // Change status of a related remote service member.
    // Should reserve a new range in zookeeper this time.
    clusterManager.setMemberStatus(remoteMember, MemberStatus.Down)
    val seq6 = SequenceRange.ranges2sequence(storage.next(1))
    val actualReservedId6 = zkClient.getLong(sequencePath(seqName))
    actualReservedId6 should be(actualReservedId4 + 1 + saveAheadSize)
    seq6(0) should be(actualReservedId6 - saveAheadSize)
  }
}

}

package com.wajam.nrv {

import cluster.StaticClusterManager
import service.{MemberStatus, ServiceMember}

class NotifiableStaticClusterManager extends StaticClusterManager {
  def setMemberStatus(member: ServiceMember, status: MemberStatus) {
    member.setStatus(status, triggerEvent = true)
  }
}

}
