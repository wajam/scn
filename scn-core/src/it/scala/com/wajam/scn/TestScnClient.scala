package com.wajam.scn

import client.{ScnClientConfig, ScnClient}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.wajam.nrv.cluster.{LocalNode, Cluster, StaticClusterManager}
import com.wajam.nrv.protocol.NrvProtocol
import storage.StorageType
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.mockito.Matchers
import org.scalatest.mock.MockitoSugar
import collection.mutable
import com.wajam.nrv.utils.timestamp.Timestamp
import org.scalatest.Matchers.{atMost => _, _}
import language.postfixOps

@RunWith(classOf[JUnitRunner])
class TestScnClient extends FunSuite with MockitoSugar with BeforeAndAfter {

  var scn: Scn = null
  var scnClient: ScnClient = null
  var cluster: Cluster = null

  before {
    val manager = new StaticClusterManager
    cluster = new Cluster(new LocalNode(Map("nrv" -> 49999, "scn" -> 50000)), manager)

    val protocol = new NrvProtocol(cluster.localNode, 10000, 100)
    cluster.registerProtocol(protocol)

    scn = new Scn("scn", Some(protocol), ScnConfig(), StorageType.MEMORY)
    cluster.registerService(scn)
    scn.addMember(0, cluster.localNode)

    cluster.start()

    scnClient = new ScnClient(scn).start()
  }

  after {
    cluster.stop()
  }

  test("get next timestamp") {
    val latch = new CountDownLatch(1)
    var res = Seq.empty[Timestamp]
    scnClient.fetchTimestamps("test", (seq, optEx) => {
      res = seq
      latch.countDown()
    }, 1, -1)

    latch.await(2, TimeUnit.SECONDS)
    assert(res.length == 1)
  }

  test("get next sequence") {
    val latch = new CountDownLatch(1)
    var res = Seq.empty[Long]
    scnClient.fetchSequenceIds("test", (seq, optEx) => {
      res = seq
      latch.countDown()
    }, 1, -1)

    latch.await(2, TimeUnit.SECONDS)
    assert(res.length == 1)
  }

  test("next sequence batching") {
    val spyScn = spy(scn)
    val scnClient = new ScnClient(spyScn, ScnClientConfig(1000)).start()

    val count = 10
    val latch = new CountDownLatch(count)

    var res = Seq.empty[Long]
    var len = 0

    for (i <- 0 to count) {
      // Should be in the same 1000ms
      scnClient.fetchSequenceIds("test_batch", (seq, optEx) => {
        res = seq
        len += seq.length
        latch.countDown()
      }, 10, 1)
    }

    latch.await()

    verify(spyScn, atMost(1)).getNextSequence(Matchers.eq("test_batch"), any(), Matchers.anyInt())
  }

  test("multiple sequence and timestamps") {
    val latch = new CountDownLatch(3)
    val res = mutable.Buffer.empty[Any]

    scnClient.fetchSequenceIds("1", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1, -1)

    scnClient.fetchTimestamps("2", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1, -1)

    scnClient.fetchSequenceIds("3", (seq, optEx) => {
      res += seq
      latch.countDown()
    }, 1, -1)

    latch.await()
    assert(res.length == 3)
  }

  test("should fail when requesting too many timestamps") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val seqFuture = scnClient.fetchTimestamps("1", Timestamp.SeqPerMs + 1, 0)
    evaluating {
      Await.result[Seq[Timestamp]](seqFuture, 1 second)
    } should produce[IllegalArgumentException]
  }

  test("fetch timestamps with future") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val seq1Future = scnClient.fetchTimestamps("1", Timestamp.SeqPerMs / 4, 0)
    val seq2Future = scnClient.fetchTimestamps("1", Timestamp.SeqPerMs / 4, 0)
    val seq3Future = scnClient.fetchTimestamps("1", Timestamp.SeqPerMs, 0)

    // The first two fetch should be batched together
    val seq1 = Await.result[Seq[Timestamp]](seq1Future, 1 second)
    val seq2 = Await.result[Seq[Timestamp]](seq2Future, 1 second)
    seq1.head.timeMs should be(seq2.head.timeMs)

    // The last fetch is not batch because asking for the max sequence per ms
    val seq3 = Await.result[Seq[Timestamp]](seq3Future, 1 second)
    seq3.size should be(Timestamp.SeqPerMs)
  }

  test("fetch sequence ids with future") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val seq1Future = scnClient.fetchSequenceIds("1", 200, 0)
    val seq2Future = scnClient.fetchSequenceIds("1", 200, 0)

    val seq1 = Await.result[Seq[Long]](seq1Future, 1 second)
    val seq2 = Await.result[Seq[Long]](seq2Future, 1 second)
    seq1.size should be(200)
    seq2.size should be(200)
    seq1.last should be(seq2.head - 1)
  }
}
