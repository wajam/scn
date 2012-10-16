package com.wajam.scn

import org.apache.commons.configuration.tree.OverrideCombiner
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration, CombinedConfiguration}
import com.wajam.nrv.Logging

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

  def getNrvZookeeperServers: String = {
    config.getString("scn.nrv.zookeeper.servers")
  }

  def getScnSequenceStorage: String = {
    config.getString("scn.storage", "memory")
  }

  def getScnTimestampSaveAheadInMs: Int = {
    config.getInt("scn.timestamp.saveahead.ms", 5000)
  }

  def getScnSequenceSaveAheadSize: Int = {
    config.getInt("scn.sequence.saveahead.unit", 100)
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
