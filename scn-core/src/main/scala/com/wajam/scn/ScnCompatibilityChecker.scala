package com.wajam.scn

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.cluster.{Cluster, LocalNode}
import com.wajam.scn.client.{ScnClientConfig, ScnClient}
import scala._
import com.wajam.scn.client.ScnClientConfig
import com.wajam.nrv.utils.timestamp.Timestamp

object ScnCompatibilityChecker extends App {

  // Setup edge cluster
  val zkClient = new ZookeeperClient("zook1.cx.wajam,zook2.cx.wajam,zook3.cx.wajam,zook4.cx.wajam,zook5.cx.wajam/live")
  val clusterManager = new ZookeeperClusterManager(zkClient)
  val node = new LocalNode(Map("nrv" -> 9701))
  val cluster = new Cluster(node, clusterManager)
  val scn = new Scn("scn", ScnConfig(), zookeeperClient = Some(zkClient))
  cluster.registerService(scn)
  cluster.start()

  val scnClient = new ScnClient(scn, ScnClientConfig())
  scnClient.start()


  def seqCallback(sequence: Seq[Long], error: Option[Exception]) {
    error match {
      case Some(e) => e.printStackTrace()
      case None => sequence.foreach(println(_))
    }
  }

  def tsCallback(timestamps: Seq[Timestamp], error: Option[Exception]) {
    seqCallback(timestamps.map(_.value), error)
  }

  println("test_sequence")
  scnClient.fetchSequenceIds("test_sequence", seqCallback, 10, 0)

  Thread.sleep(5000)

  println()
  println("test_timestamp")
  scnClient.fetchTimestamps("test_timestamp", tsCallback, 10, 0)
}
