package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.scn.SequenceRange

/**
 * Sequence storage that stores sequences in Zookeeper
 */
class ZookeeperSequenceStorage(zkClient: ZookeeperClient, name: String) extends ScnStorage[Long] {
  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/sequence", "".getBytes)
  zkClient.ensureExists("/scn/sequence/%s".format(name), "0".getBytes)

  private val MIN_BATCH_SIZE = 100
  private var lastSeq = SequenceRange(0, 1)

  def head: Long = lastSeq.from

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   * sbt
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Long] = {
    val batchSize = math.max(count, MIN_BATCH_SIZE)

    if (lastSeq.length < count) {
      lastSeq = SequenceRange(lastSeq.to, zkClient.incrementCounter("/scn/sequence/%s".format(name), batchSize, 1))
    }

    lastSeq = SequenceRange(lastSeq.from + count, lastSeq.to)
    List.range(lastSeq.from - count, lastSeq.from)
  }

}
