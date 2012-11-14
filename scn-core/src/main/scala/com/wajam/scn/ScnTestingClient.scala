package com.wajam.scn

import org.apache.log4j.PropertyConfigurator
import java.net.URL
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{Cluster, LocalNode}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.service.ActionSupportOptions
import storage.StorageType
import com.wajam.nrv.Logging

object ScnTestingClient extends App with Logging {

  PropertyConfigurator.configureAndWatch(new URL(System.getProperty("log4j.configuration")).getFile, 5000)
  val config = ScnConfiguration.fromSystemProperties

  val zookeeper = new ZookeeperClient(config.getNrvZookeeperServers)

  // Create local node
  val ports = Map("nrv" -> (config.getNrvListenPort + 1000))
  val node = new LocalNode(config.getListenAddress, ports)
  info("Local node is {}", node)

  // Create cluster
  val clusterManager = new ZookeeperClusterManager(zookeeper)
  val cluster = new Cluster(node, clusterManager, actionSupportOptions = new ActionSupportOptions())

  // Sequence number generator
  val scnStorage = config.getScnSequenceStorage
  val scnConfig = ScnConfig(config.getScnTimestampSaveAheadInMs, config.getScnTimestampSaveAheadRenewalInMs,
    config.getScnSequenceSaveAheadSize, config.getScnSequenceSeeds)
  val scn = new Scn("scn", scnConfig, StorageType.ZOOKEEPER, Some(zookeeper))
  cluster.registerService(scn)
  cluster.start()

  Thread.sleep(2000)

  val scnClient = new ScnClient(scn, ScnClientConfig(20))
  while (true) {
    scnClient.fetchSequenceIds("ScnTestingClient", (sequence: Seq[Long], exception) => {
      exception match {
        case Some(e) =>
          warn("Got an error while fetching sequence: ", e)
          throw e
        case _ =>
          info(sequence.toString())
      }
    }, 5)
    Thread.sleep(1000)
  }
}
