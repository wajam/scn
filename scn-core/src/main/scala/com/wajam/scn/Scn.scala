package com.wajam.scn

import com.wajam.nrv.service.{ActionSupportOptions, Resolver, Action, Service}
import storage._

import java.util.concurrent._
import scala.collection.JavaConversions._
import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.data.{MMigrationCatchAll, MString, MInt}

/**
 * SCN service that generates atomically increasing sequence number or uniquely increasing timestamp.
 *
 * SCN = SupraChiasmatic Nucleus (human internal clock)
 * SCN = SequenCe Number generator
 *
 * Based on: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/36726.pdf
 */
class Scn(serviceName: String = "scn",
          protocol: Option[Protocol],
          config: ScnConfig,
          storageType: StorageType.Value = StorageType.ZOOKEEPER,
          zookeeperClient: Option[ZookeeperClient] = None)
  extends Service(serviceName, new ActionSupportOptions(protocol = protocol,
    resolver = Some(new Resolver(replica = 2, tokenExtractor = Resolver.TOKEN_HASH_PARAM("name")))))
  with Logging with Instrumented {

  def this(serviceName: String,
           config: ScnConfig,
           storageType: StorageType.Value = StorageType.ZOOKEEPER,
           zookeeperClient: Option[ZookeeperClient] = None) = this(serviceName, None, config, storageType, zookeeperClient)

  lazy private val nextSequenceTime = metrics.timer("scn-getnext-time", "sequence")
  lazy private val nextTimestampTime = metrics.timer("scn-getnext-time", "timestamp")
  lazy private val nextSequenceCalls = metrics.meter("scn-getnext-calls", "scn-getnext-calls", "sequence")
  lazy private val nextTimestampCalls = metrics.meter("scn-getnext-calls", "scn-getnext-calls", "timestamp")

  lazy private val nextSequenceSuccess = metrics.meter("scn-getnext-success", "scn-getnext-success", "sequence")
  lazy private val nextTimestampSuccess = metrics.meter("scn-getnext-success", "scn-getnext-success", "timestamp")
  lazy private val nextSequenceError = metrics.meter("scn-getnext-error", "scn-getnext-error", "sequence")
  lazy private val nextTimestampError = metrics.meter("scn-getnext-error", "scn-getnext-error", "timestamp")
  lazy private val nextSequenceCallsSize = metrics.meter("scn-getnext-calls-size", "scn-getnext-calls-size", "sequence")
  lazy private val nextTimestampCallsSize = metrics.meter("scn-getnext-calls-size", "scn-getnext-calls-size", "timestamp")

  private val sequenceActors = new ConcurrentHashMap[String, SequenceActor[SequenceRange]]
  private val timestampActors = new ConcurrentHashMap[String, SequenceActor[SequenceRange]]

  // Construction argument validation
  if (storageType.eq(StorageType.ZOOKEEPER) && zookeeperClient == None)
    throw new IllegalArgumentException("Zookeeper storage type require ZookeeperClient argument.")

  private[scn] val nextTimestamp = this.registerAction(new Action("/timestamp/:name/next", msg => {
    nextTimestampCalls.mark()
    val timer = nextTimestampTime.timerContext()
    val MString(name) = msg.parameters("name")
    val MInt(nb) = msg.parameters("nb")
    nextTimestampCallsSize.mark(nb)

    val timestampActor = timestampActors.getOrElse(name, {
      val storage: ScnStorage[SequenceRange] = storageType match {
        case StorageType.ZOOKEEPER =>
          new ZookeeperTimestampStorage(zookeeperClient.get,
            name, config.timestampSaveAheadMs, config.timestampSaveAheadRenewalMs)
        case StorageType.MEMORY =>
          new InMemoryTimestampStorage()
      }
      val actor = new SequenceActor[SequenceRange](name, storage, config.maxMessageQueueSize, config.messageExpirationMs)

      Option(timestampActors.putIfAbsent(name, actor)).getOrElse({
        actor.start()
        actor
      })
    })

    timestampActor.next((seq, e) => {
      e match {
        case Some(ex) =>
          nextTimestampError.mark()
          msg.replyWithError(ex)
        case _ =>
          nextTimestampSuccess.mark()
          val hdr = Map("name" -> MString(name), "sequence" -> MMigrationCatchAll(seq))
          msg.reply(hdr, data=seq)
      }
      timer.stop()
    }, nb)
  }))

  private[scn] def getNext(sequenceName: String, action: Action, cb: (Seq[SequenceRange], Option[Exception]) => Unit, nb: Int) {

    action.call(params = Map("name" -> MString(sequenceName), "nb" -> MInt(nb)), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
      {
        // TODO: MigrationDuplicate: Remove logic for parameters
        val sequence =
          if (respMsg.hasData)
            respMsg.getData[Seq[SequenceRange]]
          else if (respMsg.parameters.contains("sequence"))
            respMsg.parameters("sequence").asInstanceOf[MMigrationCatchAll].value

        cb(sequence.asInstanceOf[Seq[SequenceRange]], None)
      }
      else
        cb(Nil, optException)
    })
  }

  private[scn] def getNextTimestamp(sequenceName: String, cb: (Seq[SequenceRange], Option[Exception]) => Unit, nb: Int) {
    getNext(sequenceName, this.nextTimestamp, cb, nb)
  }

  private[scn] val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    nextSequenceCalls.mark()
    val timer = nextSequenceTime.timerContext()
    val MString(name) = msg.parameters("name")
    val MInt(nb) = msg.parameters("nb")
    nextSequenceCallsSize.mark(nb)

    val sequenceActor = sequenceActors.getOrElse(name, {
      val storage: ScnStorage[SequenceRange] = storageType match {
        case StorageType.ZOOKEEPER =>
          new ZookeeperSequenceStorage(zookeeperClient.get, name,
            config.sequenceSaveAheadSize, this, config.sequenceSeeds.getOrElse(name, 1))
        case StorageType.MEMORY =>
          new InMemorySequenceStorage()
      }
      val actor = new SequenceActor[SequenceRange](name, storage, config.maxMessageQueueSize, config.messageExpirationMs)
      Option(sequenceActors.putIfAbsent(name, actor)).getOrElse({
        actor.start()
        actor
      })
    })

    sequenceActor.next((seq, e) => {
      e match {
        case Some(ex) =>
          nextSequenceError.mark()
          msg.replyWithError(ex)
        case _ =>
          nextSequenceSuccess.mark()
          val hdr = Map("name" -> MString(name), "sequence" -> MMigrationCatchAll(seq))
          msg.reply(hdr, data=seq)
      }
      timer.stop()
    }, nb)
  }))

  private[scn] def getNextSequence(sequenceName: String, cb: (Seq[SequenceRange], Option[Exception]) => Unit, nb: Int) {
    getNext(sequenceName, this.nextSequence, cb, nb)
  }
}
