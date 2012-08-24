package com.wajam.scn

import com.wajam.nrv.service.{Action, Service}
import storage._

import java.util.concurrent._
import scala.collection.JavaConversions._
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient

/**
 * SCN service that generates atomically increasing sequence number or uniquely increasing timestamp.
 *
 * SCN = SupraChiasmatic Nucleus (human internal clock)
 * SCN = SequenCe Number generator
 *
 * Based on: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/36726.pdf
 */
class Scn(serviceName: String = "scn", storageType: StorageType.Value = StorageType.memory,
          zookeeperClient: Option[ZookeeperClient] = None) extends Service(serviceName) {

  private val sequenceActors = new ConcurrentHashMap[String, SequenceActor[Long]]
  private val timestampActors = new ConcurrentHashMap[String, SequenceActor[Timestamp]]

  // Construction argument validation
  if (storageType.eq(StorageType.zookeeper) && zookeeperClient == None)
    throw new IllegalArgumentException("Zookeeper storage type require ZookeeperClient argument.")

  private val nextTimestamp = this.registerAction(new Action("/timestamp/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Int]

    val timestampActor = timestampActors.getOrElse(name, {
      val actor = new SequenceActor[Timestamp](storageType match {
        case StorageType.memory =>
          new InMemoryTimestampStorage()
        //case StorageType.zookeeper =>
        //  new ZookeeperTimestampStorage(zookeeperClient.get, name)
      })
      Option(timestampActors.putIfAbsent(name, actor)).getOrElse(actor)
    })

    timestampActor.next(seq => {
      msg.reply(Map("name" -> name, "sequence" -> seq))
    }, nb)
  }))

  def getNextTimestamp(name: String, cb: (List[Timestamp], Option[Exception]) => Unit, nb: Option[Int] = None) {
    this.nextTimestamp.call(params = Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("timestamp").asInstanceOf[List[Timestamp]], None)
      else
        cb(Nil, optException)
    })
  }

  private val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Int]

    val sequenceActor = sequenceActors.getOrElse(name, {
      val actor = new SequenceActor[Long](storageType match {
        case StorageType.memory =>
          new InMemorySequenceStorage()
        //case StorageType.zookeeper =>
        //  new ZookeeperSequenceStorage(zookeeperClient.get, name)
      })
      Option(sequenceActors.putIfAbsent(name, actor)).getOrElse(actor)
    })

    sequenceActor.next(seq => {
      msg.reply(Map("name" -> name, "sequence" -> seq))
    }, nb)
  }))

  def getNextSequence(name: String, cb: (List[Int], Option[Exception]) => Unit, nb: Option[Int] = None) {
    this.nextSequence.call(params = Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("sequence").asInstanceOf[List[Int]], None)
      else
        cb(Nil, optException)
    })
  }

}


