package com.wajam.scn

import com.wajam.nrv.service.{Action, Service}


/**
 * SCN service that generates atomically increasing sequence number or uniquely increasing timestamp.
 *
 * SCN = SupraChiasmatic Nucleus (human internal clock)
 * SCN = SequenCe Number generator
 *
 * Based on: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/36726.pdf
 */
class Scn(storage: SequenceStorage, serviceName: String = "scn") extends Service(serviceName) {
  private val sequenceActor = new SequenceActor(storage)
  sequenceActor.start()

  private val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    val name = msg.parameters("name").toString
    val nb = msg.parameters("nb").asInstanceOf[Option[Int]]

    sequenceActor.next(name, seq => {
      msg.reply(Map("name" -> name, "sequence" -> seq))
    }, nb)
  }))

  def getNextSequence(name: String, cb: (Int, Option[Exception]) => Unit, nb : Option[Int] = None) {
    this.nextSequence.call(params = Map("name" -> name, "nb" -> nb), onReply = (respMsg, optException) => {
      if (optException.isEmpty)
        cb(respMsg.parameters("sequence").asInstanceOf[Int], None)
      else
        cb(0, optException)
    })
  }
}
