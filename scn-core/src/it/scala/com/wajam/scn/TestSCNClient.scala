package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FunSuite, BeforeAndAfterEach, BeforeAndAfterAll}
import com.wajam.nrv.cluster.{Node, Cluster, StaticClusterManager}
import com.wajam.nrv.protocol.NrvProtocol
import storage.StorageType
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.mockito.Matchers
import org.scalatest.mock.MockitoSugar
import collection.mutable

/**
 * Description
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
@RunWith(classOf[JUnitRunner])
class TestSCNClient extends FunSuite with MockitoSugar
with BeforeAndAfterAll with BeforeAndAfterEach {

  var scn: Scn = null
  var scnClient: ScnClient = null
  var cluster: Cluster = null

  override def beforeEach() {
    val manager = new StaticClusterManager
    cluster = new Cluster(new Node("0.0.0.0", Map("nrv" -> 49999, "scn" -> 50000)), manager)

    val protocol = new NrvProtocol(cluster.localNode, cluster)
    cluster.registerProtocol(protocol)

    scn = new Scn("scn", Some(protocol), ScnConfig(), StorageType.MEMORY)
    cluster.registerService(scn)
    scn.addMember(0, cluster.localNode)

    cluster.start()

    scnClient = new ScnClient(scn)
  }

  override def afterEach() {
    cluster.stop()
  }

  test("get next timestamp") {
    val latch = new CountDownLatch(1)
    var res = Seq.empty[Timestamp]
    scnClient.getNextTimestamp("test", (seq, optEx) => {
      res = seq
      latch.countDown()
    }, 1)

    latch.await(500, TimeUnit.MILLISECONDS)
    assert(res.length == 1)
  }

  test("get next sequence") {
    val latch = new CountDownLatch(1)
    var res = Seq.empty[Long]
    scnClient.getNextSequence("test", (seq, optEx) => {
      res = seq
      latch.countDown()
    }, 1)

    latch.await(2, TimeUnit.SECONDS)
    assert(res.length == 1)
  }

  test("next sequence batching") {
    val spyScn = spy(scn)
    val scnClient = new ScnClient(spyScn, ScnClientConfig(1000))

    val count = 10
    val latch = new CountDownLatch(count)

    var res = Seq.empty[Long]
    var len = 0

    for (i <- 0 to count) {
      // Should be in the same 1000ms
      scnClient.getNextSequence("test_batch", (seq, optEx) => {
        res = seq
        len += seq.length
        latch.countDown()
      }, 10)
    }

    latch.await()

    verify(spyScn, atMost(1)).getNextSequence(Matchers.eq("test_batch"), any(), Matchers.anyInt())
  }

  test("multiple sequence and timestamps") {
    val latch = new CountDownLatch(3)
    val res = mutable.Buffer.empty[Any]

    scnClient.getNextSequence("1", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1)

    scnClient.getNextTimestamp("2", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1)

    scnClient.getNextSequence("3", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1)

    latch.await()
    assert(res.length == 3)
  }
}