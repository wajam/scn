package com.wajam.scn

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.FunSuite
import storage.ZookeeperSequenceStorage

@RunWith(classOf[JUnitRunner])
class TestZookeeperStorage extends FunSuite {
  val storage = new ZookeeperSequenceStorage(new ZookeeperClient("127.0.0.1"), "it_test")

  test("increment") {
    val range = storage.next(10)
    assert(range.last - range.head == 8, range.last - range.head)
    assert(range.last >= 10, range.last)
    assert(range.size == 10, range.size)
  }
}
