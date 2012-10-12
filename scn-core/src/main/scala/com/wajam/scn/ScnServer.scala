package com.wajam.scn

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.protocol.HttpProtocol
import com.wajam.scn.storage.StorageType
import java.net.URL
import org.apache.log4j.PropertyConfigurator
import com.wajam.nrvext.scribe.ScribeTraceRecorder
import com.wajam.nrv.tracing.NullTraceRecorder
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit

/**
 * Description
 *
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */

class ScnServer(config: ScnConfiguration) {

  // Tracing
  val traceRecorder = if (config.isTraceEnabled) {
    new ScribeTraceRecorder(config.getTraceScribeHost, config.getTraceScribePort)
  } else {
    NullTraceRecorder
  }

  val manager = new StaticClusterManager
  val node = new Node("0.0.0.0", Map("nrv" -> config.getNrvListenPort, "scn" -> config.getHttpListenPort))
  val cluster = new Cluster(node, manager)

  val scnStorage = config.getScnSequenceStorage
  val scnConfig = ScnConfig(config.getScnTimestampSaveAheadInMs, config.getScnSequenceSaveAheadSize)
  val scn = scnStorage match {
    case "memory" =>
      new Scn("scn", scnConfig, StorageType.MEMORY)
    case "zookeeper" =>
      new Scn("scn", scnConfig, StorageType.ZOOKEEPER, Some(new ZookeeperClient(config.getNrvZookeeperServers)))
  }

  val protocol = new HttpProtocol("scn", cluster.localNode, cluster)
  cluster.registerProtocol(protocol)

  cluster.registerService(scn)

  val scnMembersString = config.getScnClusterMembers
  manager.addMembers(scn, config.getScnClusterMembers)

  // Metrics binding to Graphite
  if (config.isGraphiteEnabled) {
    GraphiteReporter.enable(
      config.getGraphiteUpdatePeriodInSec, TimeUnit.SECONDS,
      config.getGraphiteServerAddress, config.getGraphiteServerPort,
      config.getEnvironment + ".scn." + node.uniqueKey)
  }


  def start() {
    cluster.start()
  }

  def startAndBlock() {
    start()
    Thread.currentThread().join()
  }

  def stop() {
    cluster.stop()
  }
}

object ScnServer extends App {
  PropertyConfigurator.configureAndWatch(new URL(System.getProperty("log4j.configuration")).getFile, 5000)
  val config = ScnConfiguration.fromSystemProperties
  new ScnServer(config).startAndBlock()
}
