package com.wajam.scn

import com.wajam.nrv.service.{Action, Service}
import storage.{InMemoryTimestampStorage, InMemorySequenceStorage, ScnStorage}
import collection.mutable.Map
import collection.immutable


/**
 * SCN service that generates atomically increasing sequence number or uniquely increasing timestamp.
 *
 * SCN = SupraChiasmatic Nucleus (human internal clock)
 * SCN = SequenCe Number generator
 *
 * Based on: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/36726.pdf
 */
class Scn(serviceName: String = "scn") extends Service(serviceName) {
  private val sequenceActors = Map[String, SequenceActor[Any]]()

  private val nextTimestamp = this.registerAction(new Action("/timestamp/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Option[Int]]

    val sequenceActor = sequenceActors.get(name) match {
      case Some(actor) =>
        actor
      case None =>
        // TODO : Check configuration for storage (ZooKeeper or InMemory)
        val actor = new SequenceActor[Timestamp](new InMemoryTimestampStorage())
        sequenceActors += (name -> actor.asInstanceOf[SequenceActor[Any]])
        actor
    }

    sequenceActor.next(seq => {
      msg.reply(immutable.Map("name" -> name, "sequence" -> seq))
    }, nb)
  }))

  def getNextTimestamp(name: String, cb: (List[Timestamp], Option[Exception]) => Unit, nb : Option[Int] = None) {
    this.nextTimestamp.call(params = immutable.Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("timestamp").asInstanceOf[List[Timestamp]], None)
      else
        cb(Nil, optException)
    })
  }

  private val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Option[Int]]

    val sequenceActor = sequenceActors.get(name) match {
      case Some(actor) =>
        actor
      case None =>
        // TODO : Check configuration for storage (ZooKeeper or InMemory)
        val actor = new SequenceActor[Int](new InMemorySequenceStorage())
        sequenceActors += (name -> actor.asInstanceOf[SequenceActor[Any]])
        actor
    }

    sequenceActor.next(seq => {
      msg.reply(immutable.Map("name" -> name, "sequence" -> seq))
    }, nb)
  }))

  def getNextSequence(name: String, cb: (List[Int], Option[Exception]) => Unit, nb : Option[Int] = None) {
    this.nextSequence.call(params = immutable.Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("sequence").asInstanceOf[List[Int]], None)
      else
        cb(Nil, optException)
    })
  }
}
