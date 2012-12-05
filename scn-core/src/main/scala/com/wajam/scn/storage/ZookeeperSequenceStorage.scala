package com.wajam.scn.storage

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.scn.SequenceRange
import com.wajam.nrv.zookeeper.service.ZookeeperService
import com.wajam.scn.storage.ZookeeperSequenceStorage._
import com.wajam.nrv.utils.CurrentTime
import math._
import com.yammer.metrics.scala.Instrumented

/**
 * Sequence storage that stores sequences in Zookeeper
 */
class ZookeeperSequenceStorage(zkClient: ZookeeperClient, name: String, minSaveAheadSize: Int, seed: Long = 1)
  extends ScnStorage[SequenceRange] with CurrentTime with Instrumented {

  private var availableSeq = SequenceRange(seed, seed)
  private val saveAhead = new DynamicSequenceSaveAhead(minSaveAheadSize, this)
  @volatile private var lastSaveAheadSize: Long = saveAhead.size

  private val incremented = metrics.meter("saveahead-incremented", "saveahead-incremented", name)
  private val saveAheadSize = metrics.gauge("saveahead-size", name) {
    lastSaveAheadSize
  }

  zkClient.ensureAllExists(sequencePath(name), seed)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[SequenceRange] = {
    saveAhead.update(count)

    if (availableSeq.length >= count) {
      List(createSequenceFromLocalAvailableSeq(count))
    } else {
      val part1 = createSequenceFromLocalAvailableSeq(availableSeq.length.toInt)
      val countToFetchFromZookeeper = count - part1.length
      val batchSize = math.max(countToFetchFromZookeeper, saveAhead.size)
      var lastReservedId = incrementZookeeperSequence(batchSize)
      var from = lastReservedId - batchSize
      if(seed > from) {
        //reseed the sequence
        lastReservedId = incrementZookeeperSequence(seed - lastReservedId + batchSize)
        from = lastReservedId - batchSize
      }
      val to = from + countToFetchFromZookeeper
      availableSeq = SequenceRange(to, lastReservedId)

      List(part1, SequenceRange(from, to))
    }
  }

  private def incrementZookeeperSequence(batchSize: Long): Long = {
    lastSaveAheadSize = batchSize
    incremented.mark()
    zkClient.incrementCounter(sequencePath(name), batchSize, seed)
  }

  private def createSequenceFromLocalAvailableSeq(count: Int): SequenceRange = {
    val (from, to) = (availableSeq.from, availableSeq.from + count)
    availableSeq = SequenceRange(to, availableSeq.to)
    SequenceRange(from, to)
  }
}

object ZookeeperSequenceStorage {
  def sequencePath(sequenceName: String) = ZookeeperService.dataPath("scn") + "/sequences/" + sequenceName
}

class DynamicSequenceSaveAhead(minSaveAheadSize: Int, timeGenerator: CurrentTime, bucketDuration: Long = 5000L) {
  var currentBucket: Int = 0
  var lastBucket: Int = 0
  var bucketStartTimestamp = timeGenerator.currentTime

  def update(count: Int) {
    val now = timeGenerator.currentTime
    if (bucketStartTimestamp + bucketDuration > now) {
      currentBucket += count
    } else {
      lastBucket = if (bucketStartTimestamp + bucketDuration * 2 > now) currentBucket else 0
      currentBucket = count
      bucketStartTimestamp = now
    }
  }

  def size: Int = {
    max((lastBucket + currentBucket) * 1.5, minSaveAheadSize).toInt
  }
}
