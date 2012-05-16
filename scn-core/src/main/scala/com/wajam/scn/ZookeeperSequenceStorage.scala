package com.wajam.scn

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient

/**
 * Sequence storage that stores
 */
class ZookeeperSequenceStorage(zkClient:ZookeeperClient) extends SequenceStorage {
  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/sequence", "".getBytes)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param name Name of the sequence
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(name: String, count: Int): (Int, Int) = {
    val counter = zkClient.incrementCounter("/scn/sequence/%s".format(name), count, 0).toInt
    (counter - count + 1, counter)
  }
}
