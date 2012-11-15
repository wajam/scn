package com.wajam.scn

import org.apache.log4j.PropertyConfigurator
import java.net.URL
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{Cluster, LocalNode}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.service.ActionSupportOptions
import storage.StorageType
import com.wajam.nrv.Logging
import java.text.SimpleDateFormat

object ScnTestingClient extends App with Logging {

  PropertyConfigurator.configureAndWatch(new URL(System.getProperty("log4j.configuration")).getFile, 5000)
  val config = ScnConfiguration.fromSystemProperties

  val zookeeper = new ZookeeperClient(config.getNrvZookeeperServers)

  // Create local node
  val ports = Map("nrv" -> (config.getNrvListenPort + 1000))
  val node = new LocalNode(ports)
  info("Local node is {}", node)

  // Create cluster
  val clusterManager = new ZookeeperClusterManager(zookeeper)
  val cluster = new Cluster(node, clusterManager, actionSupportOptions = new ActionSupportOptions())

  // Sequence number generator
  val scn = new Scn("scn", ScnConfig(), StorageType.ZOOKEEPER, Some(zookeeper))
  cluster.registerService(scn)
  cluster.start()

  // Ensure cluster is ready
  Thread.sleep(500)

  val scnClient = new ScnClient(scn, ScnClientConfig()).start()

  val workerCount = 5
  val targetTps = 10
  val sequenceSize = 10

  for (i <- 1 to workerCount) {

    // Try to spread workers execution evenly
    Thread.sleep(1000 / workerCount)

    // Fetching loop
    (new Thread(new Runnable with Logging {
      def run() {
        try {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
          var batchStart = System.currentTimeMillis
          var calls, errors = 0L
          var dur = List[Long]()
          while (true) {
            {
              var callStart = System.currentTimeMillis
              scnClient.fetchSequenceIds("ScnTestingClient", (sequence: Seq[Long], exception) => {
                exception match {
                  case Some(e) =>
                    errors += 1
                  case _ =>
                }

                val now = System.currentTimeMillis
                dur = (now - callStart) :: dur
                calls += 1

                val elapsed = now - batchStart
                if (elapsed > 5000) {
                  val msg = "%s [%d] cnt=%d, err=%d, avr=%5.3f, med=%5.3f, min=%5.3f, max=%5.3f, tps=%5.3f".format(
                    dateFormat.format(now), i, calls, errors,
                    dur.sum/calls/1000.0, dur.sorted.toList(dur.size/2)/1000.0, dur.min/1000.0, dur.max/1000.0, calls*1000.0/elapsed)
                  dur = List[Long]()
                  calls = 0L
                  errors = 0L
                  batchStart = System.currentTimeMillis
                  println(msg)
                }

              }, sequenceSize, i)
            }
            Thread.sleep(1000 / targetTps * workerCount)
          }
        } catch {
          case t: Throwable =>
            error("Got an error in worker {}: ", i, t)
        }
      }
    })).start()
  }
}
