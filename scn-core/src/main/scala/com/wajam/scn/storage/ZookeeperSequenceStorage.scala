package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient

/**
 * Sequence storage that stores sequences in Zookeeper
 */
class ZookeeperSequenceStorage(zkClient: ZookeeperClient, name: String) extends ScnStorage[Long] {
  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/sequence", "".getBytes)
  zkClient.ensureExists("/scn/sequence/%s".format(name), "0".getBytes)

  def head: Long = zkClient.getLong("/scn/sequence/%s".format(name))

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Long] = {
    val counter = zkClient.incrementCounter("/scn/sequence/%s".format(name), count, 1)
    List.range(counter - count, counter)
  }

}
