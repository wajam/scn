package com.wajam.scn

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestZookeeperStorage extends FunSuite {
  val storage = new ZookeeperSequenceStorage(new ZookeeperClient("127.0.0.1"))

  test("increment") {
    val (from, to) = storage.next("it_test", 10)
    assert((to - from) == 9, (from, to))
    assert(to >= 10, (from, to))
  }
}
