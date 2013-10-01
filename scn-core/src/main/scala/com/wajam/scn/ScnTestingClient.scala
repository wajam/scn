package com.wajam.scn

import client.{ScnClientConfig, ScnClient}
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{Cluster, LocalNode}
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.service.ActionSupportOptions
import storage.StorageType
import com.wajam.nrv.Logging
import java.text.SimpleDateFormat
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help
import org.apache.log4j.PropertyConfigurator
import java.net.URL
import com.wajam.nrv.utils.timestamp.Timestamp

object ScnTestingClient extends App with Logging {

  object Conf extends ScallopConf(args) {
    banner(
      """Usage: ./scn-core/target/start com.wajam.scn.ScnTestingClient [OPTION] TYPE NAME SERVERS
        |A testing client that request sequence numbers to a SCN server.
        |Examples: ./scn-core/target/start com.wajam.scn.ScnTestingClient sequence test_seq 127.0.0.1/local
        | """.stripMargin)

    val port = opt[Int]("port", default=Some(9695),
      descr = "NRV local listening port. Required to connect to the cluster")
    val workers = opt[Int]("workers", default=Some(5),
      descr = "number of client worker threads calling an SCN client.")
    val tps = opt[Int]("tps", default=Some(50), noshort = true,
      descr = "worker to client calls rate. This is the global rate i.e. this value is divided by the number of workers. " +
        "This is not the real rate at which calls are reaching the server. See 'rate' option.")
    val size = opt[Int]("size", default=Some(1),
      descr = "number of sequence per request to SCN client.")
    val rate = opt[Int]("rate", default=Some(10),
      descr = "client to server wait rate in milliseconds. Calls are cummulated in the SCN client for that duration " +
        "and then going out to the server as a single call.")
    val timeout = opt[Int]("timeout", default=Some(1000),
      descr = "client to server timeout. Client resend unanswered calls (see 'rate' option) until this timeout is reached")

    val seqType = trailArg[String]("TYPE", descr = "sequence type. Must be 'sequence' or 'timestamp'", required = true,
      validate = value => value match {
        case "timestamp" => true
        case "sequence" => true
        case _ => false
      })
    val name = trailArg[String]("NAME", descr = "sequence name", required = true)
    val servers = trailArg[String]("SERVERS", required = false, default = Some("127.0.0.1/local"),
      descr = "comma separated host:port pairs, each corresponding to a zk server e.g. " +
        "\"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002\". If the optional chroot suffix " +
        "is used the example would look like: \"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a\" " +
        "where the client would be rooted at \"/app/a\" and all paths would be relative to this root")

    override protected def onError(e: Throwable) {
      e match {
        case _: Help =>
          builder.printHelp()
          sys.exit(0)
        case _ =>
          println("Error: %s".format(e.getMessage))
          println()
          builder.printHelp()
          sys.exit(1)
      }
    }
  }

  PropertyConfigurator.configureAndWatch(new URL(System.getProperty("log4j.configuration")).getFile, 5000)

  // Create local node
  val ports = Map("nrv" -> (Conf.port.apply()))
  val node = new LocalNode(ports)
  info("Local node is {}", node)

  // Create cluster
  val zookeeper = new ZookeeperClient(Conf.servers.apply())
  val clusterManager = new ZookeeperClusterManager(zookeeper)
  val cluster = new Cluster(node, clusterManager, actionSupportOptions = new ActionSupportOptions())

  // Sequence number generator
  val scn = new Scn("scn", ScnConfig(), StorageType.ZOOKEEPER, Some(zookeeper))
  cluster.registerService(scn)
  cluster.start()

  // Ensure cluster is ready
  Thread.sleep(500)

  val config = ScnClientConfig(executionRateInMs = Conf.rate.apply(), timeoutInMs = Conf.timeout.apply())
  val scnClient = new ScnClient(scn, config).start()

  val workerCount = Conf.workers.apply()
  val targetTps = Conf.tps.apply()
  val sequenceSize = Conf.size.apply()

  for (i <- 1 to workerCount) {

    // Try to spread workers execution evenly
    Thread.sleep(1000 / workerCount)

    // Fetching loop
    (new Thread(new Runnable with Logging {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      var batchStart = System.currentTimeMillis
      var calls, errors = 0L
      var dur = List[Long]()

      def callback(callStart: Long, exception: Option[Exception]) {
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
      }

      def run() {
        try {
          while (true) {
            {
              var callStart = System.currentTimeMillis
              Conf.seqType.apply() match {
                case "sequence" =>
                  scnClient.fetchSequenceIds(Conf.name.apply(), (seq: Seq[Long], e) => callback(callStart, e),
                    sequenceSize, i)
                case "timestamp" =>
                  scnClient.fetchTimestamps(Conf.name.apply(), (seq: Seq[Timestamp], e) => callback(callStart, e),
                    sequenceSize, i)
              }
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
