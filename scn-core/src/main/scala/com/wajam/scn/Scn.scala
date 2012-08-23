package com.wajam.scn

import com.wajam.nrv.service.{Action, Service}
import storage.{InMemorySequenceStorage, InMemoryTimestampStorage}

import java.util.concurrent._
import scala.collection.JavaConversions._


/**
 * SCN service that generates atomically increasing sequence number or uniquely increasing timestamp.
 *
 * SCN = SupraChiasmatic Nucleus (human internal clock)
 * SCN = SequenCe Number generator
 *
 * Based on: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/36726.pdf
 */
class Scn(serviceName: String = "scn") extends Service(serviceName) {
  private val sequenceActors = new ConcurrentHashMap[String, SequenceActor[Long]]
  private val timestampActors = new ConcurrentHashMap[String, SequenceActor[Timestamp]]

  private val nextTimestamp = this.registerAction(new Action("/timestamp/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Option[Int]]

    val timestampActor = timestampActors.getOrElse(name, {
      val actor = new SequenceActor[Timestamp](new InMemoryTimestampStorage())
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
    val nb = msg.parameters("nb").asInstanceOf[Option[Int]]

    val sequenceActor = sequenceActors.getOrElse(name, {
      val actor = new SequenceActor[Long](new InMemorySequenceStorage())
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
