package com.wajam.scn.storage

import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.scn.{SequenceRange, Timestamp}
import com.wajam.nrv.Logging

/**
 *
 */
class ZookeeperTimestampStorage(zkClient: ZookeeperClient, name: String, private[storage] val saveAheadInMs: Int)
  extends ScnStorage[Timestamp] with CurrentTime with Logging {

  zkClient.ensureExists("/scn", "".getBytes)
  zkClient.ensureExists("/scn/timestamp", "".getBytes)
  zkClient.ensureExists("/scn/timestamp/%s".format(name), "0".getBytes)

  private var lastTime = -1L
  private var lastSeq = SequenceRange(0, 1)
  private var savedAhead = zkClient.getLong("/scn/timestamp/%s".format(name))

  def head: Timestamp = ScnTimestamp(savedAhead, 0)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of numbers askeds
   * @return Inclusive from and to sequence
   */
  def next(count: Int): List[Timestamp] = {
    var reqTime = currentTime

    // Avoid duplicate ID with drifting *late* clock
    if (lastTime == -1 && reqTime < savedAhead) {
      throw new Exception("Drifting late clock detected.")
    }

    if (ScnTimestamp(reqTime, 0) >= head) {
      // Save ahead
      savedAhead = reqTime + saveAheadInMs
      zkClient.set("/scn/timestamp/%s".format(name), savedAhead.toString.getBytes)
    }

    while (lastSeq.length != count) {
      lastSeq = if (lastTime == reqTime) {
        if (lastSeq.to > ScnTimestamp.MAX_SEQ_NO) {
          throw new Exception("Maximum sequence number exceeded for timestamp in this millisecond.")
        }
        SequenceRange(lastSeq.to, lastSeq.to + count)
      } else if (lastTime < reqTime) {
        SequenceRange(1, count + 1)
      } else {
        // Clock is late for some reason
        reqTime = currentTime
        SequenceRange(0, 0)
      }
    }

    lastTime = reqTime
    List.range(lastSeq.from, lastSeq.to).map(l => ScnTimestamp(lastTime, l))
  }

}
