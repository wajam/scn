package com.wajam.scn

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}
import com.wajam.nrv.cluster.{Node, Cluster, StaticClusterManager}
import com.wajam.nrv.protocol.NrvProtocol
import storage.StorageType
import java.util.concurrent.CountDownLatch
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers

/**
 * Description
 *
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class TestSCNClient extends FunSuite
with BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar {

  var scn: Scn = null
  var scnClient: ScnClient = null

  override def beforeAll() {
    val manager = new StaticClusterManager
    val cluster = new Cluster(new Node("0.0.0.0", Map("nrv" -> 49999, "scn" -> 50000)), manager)

    val protocol = new NrvProtocol(cluster.localNode, cluster)
    cluster.registerProtocol(protocol)

    scn = new Scn("scn", Some(protocol), StorageType.memory)
    cluster.registerService(scn)
    scn.addMember(0, cluster.localNode)

    cluster.start()
  }

  override def beforeEach() {
    scnClient = new ScnClient(scn)
  }

  test("get next timestamp") {
    val latch = new CountDownLatch(1)
    var res = List.empty[Timestamp]
    scnClient.getNextTimestamp("test", (seq, optEx) => {
      res = seq.asInstanceOf[List[Timestamp]]
      latch.countDown()
    }, 1)

    latch.await()
    assert(res.length == 1)
  }

  test("get next sequence") {
    val latch = new CountDownLatch(1)
    var res = List.empty[Long]
    scnClient.getNextSequence("test", (seq, optEx) => {
      res = seq.asInstanceOf[List[Long]]
      latch.countDown()
    }, 1)

    latch.await()
    assert(res.length == 1)
  }

  test("next sequence batching") {
    val spyScn = spy(scn)
    scnClient = new ScnClient(spyScn)

    val count = 10
    val latch = new CountDownLatch(count)

    var res = List.empty[Long]
    for (i <- 1 to count) {
      // Should be in the same 10ms
      scnClient.getNextSequence("test_batch", (seq, optEx) => {
        res = seq.asInstanceOf[List[Long]]
        latch.countDown()
      }, 10)
    }

    latch.await()
    verify(spyScn, times(1)).getNextSequence(Matchers.eq("test_batch"), Matchers.any(), Matchers.anyInt())
  }

}

