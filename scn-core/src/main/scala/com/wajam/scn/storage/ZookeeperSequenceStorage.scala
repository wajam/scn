package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient

/**
 * Sequence storage that stores
 */
class ZookeeperSequenceStorage(zkClient: ZookeeperClient, name: String) extends ScnStorage[Int] {
  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/sequence", "".getBytes)

  def head: Int = zkClient.getInt("/scn/sequence/%s".format(name))

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Int] = {
    val counter = zkClient.incrementCounter("/scn/sequence/%s".format(name), count, 0).toInt
    List.range(counter - count + 1, counter)
  }



}
