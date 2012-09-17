import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import com.wajam.nrv.cluster.{StaticClusterManager, Node, Cluster}
import com.wajam.nrv.protocol.HttpProtocol
import com.wajam.scn.{ScnConfig, Scn}
import com.wajam.scn.storage.StorageType

/**
 * Description
 *
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
object ScnHTTPStandalone {

  def main(args: Array[String]) {
    val manager = new StaticClusterManager
    val cluster = new Cluster(new Node("0.0.0.0", Map("nrv" -> 49999, "scn" -> 50000)), manager)

    val protocol = new HttpProtocol("scn", cluster.localNode, cluster)
    cluster.registerProtocol(protocol)

    val scn = new Scn("scn", Some(protocol), ScnConfig(), StorageType.ZOOKEEPER, Some(new ZookeeperClient("127.0.0.1")))
    cluster.registerService(scn)
    scn.addMember(0, cluster.localNode)

    cluster.start()
  }

}
