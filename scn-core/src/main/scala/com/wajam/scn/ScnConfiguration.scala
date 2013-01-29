package com.wajam.scn

import org.apache.commons.configuration.tree.OverrideCombiner
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration, CombinedConfiguration}
import com.wajam.nrv.Logging
import scala.collection.JavaConversions._

/**
 *
 */
class ScnConfiguration(config: Configuration) {

  def getEnvironment: String = {
    config.getString("scn.environment")
  }

  def getListenAddress: String = {
    config.getString("scn.listen.address", "0.0.0.0")
  }

  def getNrvListenPort: Int = {
    config.getInt("scn.nrv.listen.port")
  }

  def getNrvClusterManager: String = {
    config.getString("scn.nrv.cluster_manager")
  }

  def getNrvZookeeperServers: String = {
    config.getStringArray("scn.nrv.zookeeper.servers").mkString(",")
  }

  def getNrvProtocolVersion: Int = {
    config.getInt("scn.nrv.nrv_protocol.version", 1)
  }

  def getScnSequenceStorage: String = {
    config.getString("scn.storage", "memory")
  }

  def getScnTimestampSaveAheadInMs: Int = {
    config.getInt("scn.timestamp.saveahead.ms", 5000)
  }

  def getScnTimestampSaveAheadRenewalInMs: Int = {
    config.getInt("scn.timestamp.renewal.ms", 1000)
  }

  def getScnSequenceSaveAheadSize: Int = {
    config.getInt("scn.sequence.saveahead.unit", 1000)
  }

  def getScnMessageMaxQueueSize: Int = {
    config.getInt("scn.message.max_queue_size", 1000)
  }

  def getScnMessageExpirationMs: Int = {
    config.getInt("scn.message.expiration.ms", 250)
  }

  def getScnSequenceSeeds: Map[String, Long] = {
    val scnSequenceSeedCfg = config.subset("scn.sequence.seed")
    val keys: Seq[String] = scnSequenceSeedCfg.getKeys.asInstanceOf[java.util.Iterator[String]].toSeq
    keys.map(key => (key, scnSequenceSeedCfg.getLong(key))).toMap
  }

  def getScnClusterMembers: Seq[String] = {
    Seq(config.getStringArray("scn.cluster.members"): _*)
  }

  def getGraphiteServerAddress: String = {
    config.getString("scn.graphite.server.address")
  }

  def getGraphiteServerPort: Int = {
    config.getInt("scn.graphite.server.port")
  }

  def getGraphiteUpdatePeriodInSec: Int = {
    config.getInt("scn.graphite.update.period.sec")
  }

  def isGraphiteEnabled: Boolean = {
    config.getBoolean("scn.graphite.enabled", false)
  }

  def getNrvSwitchboardNumExecutors: Int = {
    config.getInt("scn.nrv.switchboard.num_executors", 200)
  }

  def isTraceEnabled: Boolean = {
    config.getBoolean("scn.trace.enabled", false)
  }

  def getTraceScribeHost: String = {
    config.getString("scn.trace.scribe.host")
  }

  def getTraceScribePort: Int = {
    config.getInt("scn.trace.scribe.port", 1463)
  }

  def getTraceScribeSamplingRate: Int = {
    config.getInt("scn.trace.scribe.sampling_rate", 1000)
  }
}

object ScnConfiguration extends Logging {
  def fromSystemProperties: ScnConfiguration = {
    val confPath = System.getProperty("scn.config")

    log.info("Using configuration " + confPath)

    val config = new CombinedConfiguration(new OverrideCombiner())
    val envConfig = new PropertiesConfiguration(confPath)
    config.addConfiguration(envConfig)

    val defaultConfig = new PropertiesConfiguration("etc/default.properties")
    config.addConfiguration(defaultConfig)
    new ScnConfiguration(config)
  }
}
