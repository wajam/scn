package com.wajam.scn

import com.wajam.nrv.service.{Resolver, Action, Service}
import storage._

import java.util.concurrent._
import scala.collection.JavaConversions._
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.nrv.protocol.Protocol
import com.wajam.nrv.Logging

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
          storageType: StorageType.Value = StorageType.zookeeper,
          zookeeperClient: Option[ZookeeperClient] = None)

  extends Service(serviceName, protocol, Some(new Resolver(tokenExtractor = Resolver.TOKEN_HASH_PARAM("name")))) with Logging {

  def this(serviceName: String,
           storageType: StorageType.Value = StorageType.zookeeper,
           zookeeperClient: Option[ZookeeperClient] = None) = this(serviceName, None, storageType, zookeeperClient)

  private val sequenceActors = new ConcurrentHashMap[String, SequenceActor[Long]]
  private val timestampActors = new ConcurrentHashMap[String, SequenceActor[Timestamp]]

  // Construction argument validation
  if (storageType.eq(StorageType.zookeeper) && zookeeperClient == None)
    throw new IllegalArgumentException("Zookeeper storage type require ZookeeperClient argument.")

  private[scn] val nextTimestamp = this.registerAction(new Action("/timestamp/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").toString.toInt

    val timestampActor = timestampActors.getOrElse(name, {
      val actor = new SequenceActor[Timestamp](storageType match {
        case StorageType.zookeeper =>
          new ZookeeperTimestampStorage(zookeeperClient.get, name)
        case StorageType.memory =>
          new InMemoryTimestampStorage()
      })
      actor.start()
      Option(timestampActors.putIfAbsent(name, actor)).getOrElse(actor)
    })

    timestampActor.next(seq => {
      val hdr = Map("name" -> name, "sequence" -> seq)
      msg.reply(hdr)
    }, nb)
  }))

  def getNextTimestamp(name: String, cb: (List[Timestamp], Option[Exception]) => Unit, nb: Int) {
    this.nextTimestamp.call(params = Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("sequence").asInstanceOf[List[Timestamp]], None)
      else
        cb(Nil, optException)
    })
  }

  private[scn] val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").toString.toInt

    val sequenceActor = sequenceActors.getOrElse(name, {
      val actor = new SequenceActor[Long](storageType match {
        case StorageType.zookeeper =>
          new ZookeeperSequenceStorage(zookeeperClient.get, name)
        case StorageType.memory =>
          new InMemorySequenceStorage()
      })
      actor.start()
      Option(sequenceActors.putIfAbsent(name, actor)).getOrElse(actor)
    })

    sequenceActor.next(seq => {
      val hdr = Map("name" -> name, "sequence" -> seq)
      msg.reply(hdr)
    }, nb)
  }))

  def getNextSequence(name: String, cb: (List[Long], Option[Exception]) => Unit, nb: Int) {
    this.nextSequence.call(params = Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("sequence").asInstanceOf[List[Long]], None)
      else
        cb(Nil, optException)
    })
  }

}


