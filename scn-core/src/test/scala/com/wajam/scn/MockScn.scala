package com.wajam.scn

import storage.StorageType
import com.wajam.nrv.Logging

/**
 * 
 */
class MockScn extends Scn(null, null, StorageType.MEMORY) with Logging {
  var nextSequenceSeq: Seq[Seq[SequenceRange]] = Nil
  var nextTimestampSeq: Seq[Seq[SequenceRange]] = Nil
  var exception: Option[Exception] = None

  override private[scn] def getNextTimestamp(name: String, cb: (Seq[SequenceRange], Option[Exception]) => Unit, nb: Int) {
    val nextSeq = nextTimestampSeq match {
      case Nil => null
      case seq => {
        val current = seq.head
        nextTimestampSeq = seq.tail
        current
      }
    }
    cb(nextSeq, exception)
  }

  override private[scn] def getNextSequence(name: String, cb: (Seq[SequenceRange], Option[Exception]) => Unit, nb: Int) {
    val nextSeq = nextSequenceSeq match {
      case Nil => null
      case seq => {
        val current = seq.head
        nextSequenceSeq = seq.tail
        current
      }
    }
    info("getNextSequence: head={}, tail={}", nextSeq, nextSequenceSeq)
    cb(nextSeq, exception)
  }
}