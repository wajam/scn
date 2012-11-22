package com.wajam.scn.storage

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.scn.{ScnTimestamp, SequenceRange, Timestamp}
import com.wajam.nrv.Logging
import com.wajam.nrv.utils.CurrentTime
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException
import com.wajam.nrv.zookeeper.service.ZookeeperService
import com.wajam.scn.storage.ZookeeperTimestampStorage._

/**
 * Sequence storage that stores timestamps in Zookeeper
 */
class ZookeeperTimestampStorage(zkClient: ZookeeperClient, name: String, private[storage] val saveAheadInMs: Int,
                                saveAheadRenewalInMs: Int)
  extends ScnStorage[Timestamp] with CurrentTime with Logging {

  zkClient.ensureAllExists(timestampPath(name), timestamp2string(-1L))

  private var lastTime = -1L
  private var lastSeq = SequenceRange(0, 1)
  private var lastStat = new Stat
  private var savedAhead = string2timestamp(zkClient.getString(timestampPath(name), stat = Some(lastStat)))

  protected[storage] def saveAheadTimestamp: Timestamp = ScnTimestamp(savedAhead, 0)

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

    // Save ahead update
    if (ScnTimestamp(reqTime, 0) >= ScnTimestamp(savedAhead - saveAheadRenewalInMs, 0) ) {
      try {
        // Try to persist save ahead
        zkClient.set(timestampPath(name), timestamp2string(reqTime + saveAheadInMs), lastStat.getVersion)
      } catch {
        // Our save ahead version is out of date! Another instance is generating the timestamps!
        case e: KeeperException.BadVersionException =>
          // Reset our last known serve time as this instance We cannot generate new timetsamps until save ahead is
          // expired.
          lastTime = -1L

          // Avoid duplicate ID with concurent generation
          throw new Exception("Concurent timestamps generation detected.", e)
      }
      finally {
        // Need to get the latest save ahead value and version no matter if save ahead persistence was successful or not
        savedAhead = string2timestamp(zkClient.getString(timestampPath(name), stat = Some(lastStat)))
      }
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

object ZookeeperTimestampStorage {
  def timestampPath(timestampName: String) = ZookeeperService.dataPath("scn") + "/timestamps/" + timestampName
}

