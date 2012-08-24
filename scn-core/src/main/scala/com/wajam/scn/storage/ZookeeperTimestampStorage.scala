package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.scn.Timestamp

/**
 *
 */
class ZookeeperTimestampStorage(zkClient: ZookeeperClient, name: String) extends ScnStorage[Timestamp] with CurrentTime {
  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/timestamp", "".getBytes)
  zkClient.ensureExists("/scn/timestamp/%s".format(name), ScnTimestamp(getCurrentTime).toString.getBytes)

  private var lastTime = getCurrentTime
  private var lastSeq = SequenceRange(0, 1)

  val SAVE_AHEAD_MS = 6000

  def head: Timestamp = {
    val ts = zkClient.getString("/scn/timestamp/%s".format(name))
    ScnTimestamp(ts.substring(0, ts.length() - 4).toLong, ts.substring(ts.length - 4).toLong)
  }

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers asked
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Timestamp] = {
    val reqTime = getCurrentTime

    // Save ahead X seconds
    if (reqTime >= head) {
      zkClient.set("/scn/timestamp/%s".format(name), ScnTimestamp(reqTime + SAVE_AHEAD_MS).toString.getBytes)
    }

    lastSeq = if (lastTime == reqTime) {
      SequenceRange(lastSeq.to, lastSeq.to + count)
    } else {
      SequenceRange(1, count + 1)
    }

    lastTime = reqTime
    List.range(lastSeq.from, lastSeq.to).map(l => ScnTimestamp(lastTime, l))
  }

}
