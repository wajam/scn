package com.wajam.scn

import client.{ScnClientConfig, ScnClient}
import org.scalatest.{BeforeAndAfter, FunSuite}
import storage.StorageType
import com.wajam.nrv.cluster.{Node, Cluster, LocalNode, TestingClusterInstance}
import com.wajam.nrv.tracing.Tracer
import com.wajam.nrv.service.{MemberStatus, ActionSupportOptions, Resolver}
import com.wajam.nrv.zookeeper.cluster.{ZookeeperTestingClusterDriver, ZookeeperClusterManager}
import com.wajam.nrv.utils.NullLogging
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.Logging
import com.wajam.nrv.scribe.ScribeTraceRecorder
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TestScn extends FunSuite with BeforeAndAfter {

  var testCluster: TestSequenceCluster = _
  object Log extends Logging with NullLogging

  before {
    ZookeeperTestingClusterDriver.cleanupZookeeper()
  }

  after {
    if (testCluster != null) {
      testCluster.stop()
      testCluster = null
    }
  }

  def createClusterInstance(size: Int, i: Int, manager: ZookeeperClusterManager): TestingClusterInstance = {

    val tracer = new Tracer(new ScribeTraceRecorder("127.0.0.1", 1463))
    val node = new LocalNode(Map("nrv" -> (50000 + 10 * i), "scn" -> (50002 + 10 * i)))
    val cluster = new Cluster(node, manager, new ActionSupportOptions(tracer = Option(tracer)))

    val token = Resolver.MAX_TOKEN / size * i

    val scn = new Scn("scn", ScnConfig(), StorageType.ZOOKEEPER, Option(manager.zk))
    cluster.registerService(scn)
    scn.addMember(token, cluster.localNode)

    // Increase client timeout during test to prevent client timeout during test failover
    val scnClient = new ScnClient(scn, ScnClientConfig(timeoutInMs = 5000)).start()
    new TestingClusterInstance(cluster, scnClient)
  }

  class TestSequenceCluster(val sequenceName: String, val clusterSize: Int = 5) {
    implicit val driver = new ZookeeperTestingClusterDriver((size, i, manager) => createClusterInstance(size, i, manager))

    private val stopLatch = Promise[Boolean]
    private var workers = List[Future[Seq[Long]]]()

    val token = Resolver.hashData(sequenceName)

    private def allNodes = driver.instances(0).cluster.services("scn").resolveMembers(token, clusterSize).map(_.node)

    def clientNode = allNodes(clusterSize - 1)

    def nodes = allNodes.filter(_ != clientNode)

    // Use the SCN client from the last node. This node will stay up during the whole test
    def scnClient = getInstance(clientNode).data.asInstanceOf[ScnClient]

    def start() = {
      Log.info("### Starting test cluster")
      driver.init(clusterSize)

      Log.info("### Test cluster started. Token={}. First replica node={}. Client node={}", token, nodes(0), clientNode)

      // Warm-up SCN server
      val warmup = Promise[Boolean]
      scnClient.fetchSequenceIds(sequenceName, (sequence: Seq[Long], exception) => {
        warmup.success(true)
      }, 1, -1)
      Await.result(warmup.future, Duration.Inf)

      // Create multiple workers which fetch new sequences until asked to stop
      val workerCount = 5
      val sleepDuration = 250
      for (i <- 1 to workerCount) {

        // Try to spread workers execution evenly
        Thread.sleep(sleepDuration / workerCount)

        // Fetching loop
        val p = Promise[Seq[Long]]
        workers = p.future :: workers
        Future({
          var result = List[Long]()
          while (!stopLatch.future.isCompleted && !p.future.isCompleted) {
            Log.info("### Fetching sequence for worker {}", i)
            scnClient.fetchSequenceIds(sequenceName, (sequence: Seq[Long], exception) => {
              exception match {
                case Some(e) =>
                  Log.info("### Got exception {} for worker {}", e, i)
                  p.tryFailure(e)
                case _ =>
                  Log.info("### Got sequence {} for worker {}", sequence, i)
                  result = result ::: sequence.toList
              }
            }, 1, i)
            Thread.sleep(sleepDuration)
          }

          Log.info("### Exit worker {} with result {}", i, result)
          p.trySuccess(result)
          result
        })
      }

      this
    }

    def stop() = {
      Log.info("### STOPING")
      stopLatch.success(true)

      // Collect results
      val allSequences = workers.flatMap(Await.result(_, Duration.Inf))
      allSequences.size should be(allSequences.distinct.size)
      Log.info("### STOPED")

      driver.destroy()

      allSequences
    }

    def getInstance(node: Node) = {
      driver.instances.filter(_.cluster.localNode == node).head
    }

    def waitForStatus(watchedNodes: Seq[Node], status: MemberStatus) {
      waitForStatus(clientNode, watchedNodes, status)
    }

    def waitForStatus(watchingNode: Node, watchedNodes: Seq[Node], status: MemberStatus) {
      val members = getInstance(watchingNode).cluster.services.values.flatMap(_.members)
      val watchedMembers = members.filter(member => watchedNodes.contains(member.node))
      driver.waitForCondition[Boolean](watchedMembers.forall(_.status == status), _ == true)
    }

    def printStatus() {
      for (node <- allNodes) {
        val cluster = getInstance(node).cluster
        cluster.services.values.foreach(service => Log.info("\nLocal node: {}\n{}", cluster.localNode, service.printService))
      }

    }
  }

  test("client should not be affected when all scn members but the first replica goes down and up due to zk") {
    testCluster = new TestSequenceCluster("test_sequence").start()

    Log.info("### Working before ZK close")
    Thread.sleep(1000)

    // Close zk of all but first replica
    for (instance <- testCluster.nodes.tail.map(testCluster.getInstance(_))) {
      Log.info("### Close ZK for {}", instance.cluster.localNode)
      instance.zkClient.close()
    }

    testCluster.waitForStatus(testCluster.nodes.tail, MemberStatus.Down)
    Log.info("### Working further after ZK closed")
    Thread.sleep(1000)

    // Reconnect all closed zk
    for (instance <- testCluster.nodes.tail.map(testCluster.getInstance(_))) {
      Log.info("### Connect ZK for {}", instance.cluster.localNode)
      instance.zkClient.connect()
    }

    testCluster.waitForStatus(testCluster.nodes, MemberStatus.Up)
    Log.info("### Working further more after ZK connected")
    Thread.sleep(1000)

    val cluster = testCluster
    testCluster = null
    cluster.stop()
  }

  test("client should not be affected when the first scn replica goes down and up due to zk") {
    testCluster = new TestSequenceCluster("test_sequence", 3).start()

    Log.info("### Working before ZK close")
    Thread.sleep(1000)

    // Disconnect zk of first replica
    val instance = testCluster.getInstance(testCluster.nodes(0))
    Log.info("### Close ZK for {}", instance.cluster.localNode)
    instance.zkClient.close()

    testCluster.waitForStatus(Seq(testCluster.nodes(0)), MemberStatus.Down)
    Log.info("### Working further after ZK closed")
    Thread.sleep(1000)

    // Reconnect zk of first replica
    instance.zkClient.connect()

    testCluster.waitForStatus(Seq(testCluster.nodes(0)), MemberStatus.Up)
    Log.info("### Working further more after ZK connected")
    Thread.sleep(1000)

    val cluster = testCluster
    testCluster = null
    cluster.stop()
  }

  test("zookeeper storage construction (with client failure)") {
    intercept[IllegalArgumentException] {
      new Scn("scn", ScnConfig(), StorageType.ZOOKEEPER)
    }
  }
}
