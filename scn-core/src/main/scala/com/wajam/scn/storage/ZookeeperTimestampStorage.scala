package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.scn.Timestamp
import com.wajam.nrv.Logging

/**
 *
 */
class ZookeeperTimestampStorage(zkClient: ZookeeperClient, name: String) extends ScnStorage[Timestamp] with CurrentTime with Logging {

  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/timestamp", "".getBytes)
  zkClient.ensureExists("/scn/timestamp/%s".format(name), "0".getBytes)

  private var lastTime = -1L
  private var lastSeq = SequenceRange(0, 1)
  private var savedAhead = zkClient.getLong("/scn/timestamp/%s".format(name))

  val SAVE_AHEAD_MS = 6000

  def head: Timestamp = ScnTimestamp(savedAhead, 0)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers askeds
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Timestamp] = {
    var reqTime = getCurrentTime

    // Avoid duplicate ID with drifting *late* clock
    while (lastTime == -1 && reqTime < savedAhead) {
      val waitTime = savedAhead - reqTime
      debug("Sleeping %s seconds (drifting late clock)".format(waitTime))
      Thread.sleep(waitTime)
      reqTime = getCurrentTime
    }

    if (ScnTimestamp(reqTime, 0) >= head) {
      // Save ahead
      savedAhead = reqTime + SAVE_AHEAD_MS
      zkClient.set("/scn/timestamp/%s".format(name), savedAhead.toString.getBytes)
    }

    while (lastSeq.range != count) {
      lastSeq = if (lastTime == reqTime) {
        SequenceRange(lastSeq.to, lastSeq.to + count)
      } else if (lastTime < reqTime) {
        SequenceRange(1, count + 1)
      } else {
        // Clock is late for some reason
        reqTime = getCurrentTime
        SequenceRange(0, 0)
      }
    }

    lastTime = reqTime
    List.range(lastSeq.from, lastSeq.to).map(l => ScnTimestamp(lastTime, l))
  }

}
