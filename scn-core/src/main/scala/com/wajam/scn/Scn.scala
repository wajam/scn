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

  val nextSequence = this.registerAction(new Action("/sequence/:name/next", msg => {
    val name = msg.parameters("name").toString
    val by = msg.parameters.getOrElse("increment", "1").toString.toInt
    val seq = storage.next(name, by)
    msg.reply(Map("name" -> name, "sequence" -> seq))
  }))
}
