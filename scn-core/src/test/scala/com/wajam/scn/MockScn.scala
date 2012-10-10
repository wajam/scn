package com.wajam.scn

import storage.StorageType

/**
 * 
 */
class MockScn extends Scn(null, null, StorageType.MEMORY) {
  var nextSequenceSeq: Seq[Long] = null
  var nextTimestampSeq: Seq[Timestamp] = null
  var exception: Option[Exception] = None

  override private[scn] def getNextTimestamp(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int) {
    cb(nextTimestampSeq, exception)
  }

  override private[scn] def getNextSequence(name: String, cb: (Seq[Long], Option[Exception]) => Unit, nb: Int) {
    cb(nextSequenceSeq, exception)
  }
}