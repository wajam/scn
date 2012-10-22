package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient._
import com.wajam.scn.SequenceRange

/**
 * Sequence storage that stores sequences in Zookeeper
 */
class ZookeeperSequenceStorage(zkClient: ZookeeperClient, name: String, saveAheadSize: Int, seed: Long = 1)
  extends ScnStorage[Long] {
  zkClient.ensureExists("/scn", "")
  zkClient.ensureExists("/scn/sequence", "")
  zkClient.ensureExists("/scn/sequence/%s".format(name), seed)

  private var availableSeq = SequenceRange(seed, seed)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Long] = {
    if (availableSeq.length >= count) {
      createSequenceFromLocalAvailableSeq(count)
    } else {
      val part1 = createSequenceFromLocalAvailableSeq(availableSeq.length.toInt)
      val countToFetchFromZookeeper = count - part1.length
      val batchSize = math.max(countToFetchFromZookeeper, saveAheadSize)
      var lastReservedId = zkClient.incrementCounter("/scn/sequence/%s".format(name), batchSize, seed)
      var from = lastReservedId - batchSize
      if(seed > from) {
        //reseed the sequence
        zkClient.incrementCounter("/scn/sequence/%s".format(name), seed - lastReservedId , seed)
        lastReservedId = zkClient.incrementCounter("/scn/sequence/%s".format(name), batchSize, seed)
        from = lastReservedId - batchSize
      }
      val to = from + countToFetchFromZookeeper
      availableSeq = SequenceRange(to, lastReservedId)
      part1 ::: List.range(from, to)
    }
  }

  private def createSequenceFromLocalAvailableSeq(count: Int): List[Long] = {
    val (from, to) = (availableSeq.from, availableSeq.from + count)
    availableSeq = SequenceRange(to, availableSeq.to)
    List.range(from, to)
  }
}
