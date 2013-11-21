package com.wajam.scn.storage

import com.wajam.nrv.zookeeper.ZookeeperClient
import com.wajam.nrv.zookeeper.ZookeeperClient._
import com.wajam.scn.SequenceRange
import com.wajam.commons.Logging
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException
import com.wajam.nrv.zookeeper.service.ZookeeperService
import com.wajam.scn.storage.ZookeeperTimestampStorage._
import com.wajam.commons.CurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Sequence storage that stores timestamps in Zookeeper
 */
class ZookeeperTimestampStorage(zkClient: ZookeeperClient, name: String, saveAheadInMs: Int,
                                saveAheadRenewalInMs: Int, clock: CurrentTime = new CurrentTime {})
  extends ScnStorage[SequenceRange] with Logging {

  zkClient.ensureAllExists(timestampPath(name), timestamp2string(-1L))

  private var lastTime = -1L
  private var lastTimeEndSeq: Int = 0
  private var lastStat = new Stat
  private var savedAhead = string2timestamp(zkClient.getString(timestampPath(name), stat = Some(lastStat)))

  private[storage] def saveAheadTimestamp: Timestamp = Timestamp(savedAhead, 0)

  /**
   * Get next sequence boundaries for given count.
   * WARNING: Calls to this function must be synchronized or single threaded
   *
   * @param count Number of timestamps to generate
   */
  def next(count: Int): List[SequenceRange] = {
    var now = clock.currentTime

    // Avoid duplicate ID with drifting *late* clock
    if (lastTime == -1 && now < savedAhead) {
      throw new Exception("Drifting late clock detected.")
    }

    // Save ahead update
    if (now >= savedAhead - saveAheadRenewalInMs) {
      try {
        // Try to persist save ahead
        zkClient.set(timestampPath(name), timestamp2string(now + saveAheadInMs), lastStat.getVersion)
      } catch {
        // Our save ahead version is out of date! Another instance is generating the timestamps!
        case e: KeeperException.BadVersionException =>
          // Reset our last known serve time as this instance We cannot generate new timestamps until save ahead is
          // expired.
          lastTime = -1L

          // Avoid duplicate ID with concurrent generation
          throw new Exception("Concurrent timestamps generation detected.", e)
      }
      finally {
        // Need to get the latest save ahead value and version no matter if save ahead persistence was successful or not
        savedAhead = string2timestamp(zkClient.getString(timestampPath(name), stat = Some(lastStat)))
      }
    }

    if (lastTime > now) {
      // Clock is late for some reason
      lastTimeEndSeq = 0
      Nil
    } else {
      // If already returned some timestamps in the same ms, continue the sequence for this ms. Never overflow the
      // sequence whether this is the same ms or a new ms.
      val startSeq = if (lastTime == now) lastTimeEndSeq else 0
      val endSeq = math.min(Timestamp.SeqPerMs, startSeq + count)
      val startTs = Timestamp(now, startSeq)
      lastTimeEndSeq = endSeq
      lastTime = now
      List(SequenceRange(startTs.value, startTs.value + endSeq - startSeq))
    }
  }
}

object ZookeeperTimestampStorage {
  def timestampPath(timestampName: String) = ZookeeperService.dataPath("scn") + "/timestamps/" + timestampName
}

