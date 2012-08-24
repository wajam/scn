package com.wajam.scn.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestZookeeperSequenceStorage extends FunSuite {
  val storage = new ZookeeperSequenceStorage(new ZookeeperClient("127.0.0.1"), "it_seq_test")

  test("increment") {
    val range = storage.next(10)

    assert(range.last - range.head == 9, range.last - range.head)
    assert((storage.head + 10) - range.last >= 10, (storage.head + 10) - range.last)
    assert(range.size == 10, range.size)
  }

  test("unicity of generated ids") {
    val unique = storage.next(10) ::: storage.next(20) ::: storage.next(30)

    assert(unique == unique.distinct, unique)
    assert(unique.size === 60, unique.size)
  }

  test("counter head position in zookeeper") {
    val head = storage.head

    storage.next(10)

    assert(storage.head === head + 10, storage.head)
  }


}
