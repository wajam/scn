package com.wajam.scn

import scala.concurrent.ExecutionContext

import com.twitter.concurrent.NamedPoolThreadFactory

import com.wajam.nrv.extension.json.codec.JsonCodec
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{LocalNode, StaticClusterManager, Cluster}
import com.wajam.scn.storage.StorageType
import java.net.URL
import org.apache.log4j.PropertyConfigurator
import com.wajam.tracing.{Tracer, NullTraceRecorder}
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.{Executors, TimeUnit}
import com.wajam.commons.Logging
import com.wajam.nrv.service.{Service, ActionSupportOptions}
import com.wajam.nrv.scribe.ScribeTraceRecorder
import com.wajam.nrv.protocol.HttpProtocol

class ScnServer(config: ScnConfiguration)(implicit val ec: ExecutionContext) extends Logging {
  self =>

  // Tracing
  val traceRecorder = if (config.isTraceEnabled) {
    new ScribeTraceRecorder(config.getTraceScribeHost, config.getTraceScribePort)
  } else {
    NullTraceRecorder
  }

  // Zookeeper instance (lazy loaded since unit tests won't use it)
  lazy val zookeeper = new ZookeeperClient(config.getNrvZookeeperServers)

  // Create local node
  val ports = Map("nrv" -> config.getNrvListenPort, "http" -> config.getHttpListenPort)
  val node = new LocalNode(config.getListenAddress, ports)
  log.info("Local node is {}", node)

  // Create cluster
  val clusterManager = config.getNrvClusterManager match {
    case "static" => new StaticClusterManager
    case "zookeeper" => new ZookeeperClusterManager(zookeeper)
  }

  val cluster = new Cluster(node, clusterManager,
    actionSupportOptions = new ActionSupportOptions(tracer = Some(new Tracer(traceRecorder, samplingRate = config.getTraceSamplingRate))))

  // Sequence number generator
  val scnStorage = config.getScnSequenceStorage
  val scnConfig = ScnConfig(config.getScnTimestampSaveAheadInMs, config.getScnTimestampSaveAheadRenewalInMs,
    config.getScnSequenceSaveAheadSize, config.getScnMessageMaxQueueSize, config.getScnMessageExpirationMs,
    config.getScnSequenceSeeds)
  val scn = scnStorage match {
    case "memory" =>
      new Scn("scn", scnConfig, StorageType.MEMORY)
    case "zookeeper" =>
      new Scn("scn", scnConfig, StorageType.ZOOKEEPER, Some(zookeeper))
  }
  cluster.registerService(scn)

  val httpProtocol = createHttpProtocol()
  cluster.registerProtocol(httpProtocol)

  val apiService = createApiService()
  apiService.applySupport(supportedProtocols = Some(Set(httpProtocol)))
  cluster.registerService(apiService)

  def createApiService(): Service = {
    new Service("scn-api") with ScnApi {
      def ec: ExecutionContext = self.ec

      val scn: Scn = self.scn
    }
  }

  def createHttpProtocol(): HttpProtocol = {
    val httpProtocol = new HttpProtocol("http",
      node,
      config.getConnectionTimeoutMs,
      config.getConnectionPoolMaxSize)
    httpProtocol.registerCodec("application/json", new JsonCodec)
    httpProtocol
  }

  val scnMembersString = config.getScnClusterMembers
  clusterManager match {
    case static: StaticClusterManager => static.addMembers(scn, config.getScnClusterMembers)
    case _ =>
  }

  // Metrics binding to Graphite
  if (config.isGraphiteEnabled) {
    GraphiteReporter.enable(
      config.getGraphiteUpdatePeriodInSec, TimeUnit.SECONDS,
      config.getGraphiteServerAddress, config.getGraphiteServerPort,
      config.getEnvironment + ".scn." + node.uniqueKey.replace(".", "-"))
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

object ScnServer extends App with Logging {

  try {
    PropertyConfigurator.configureAndWatch(new URL(System.getProperty("log4j.configuration")).getFile, 5000)
    val config = ScnConfiguration.fromSystemProperties

    implicit val ec = ExecutionContext.fromExecutorService(
      Executors.newFixedThreadPool(config.getExecutionContextPoolSize, new NamedPoolThreadFactory("scn-thread")))

    new ScnServer(config).startAndBlock()
  } catch {
    case e: Exception => {
      error("Fatal error starting Scn server.", e)
      System.exit(1)
    }
  }

}
