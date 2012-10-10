package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import java.util.concurrent.CountDownLatch
import storage.{InMemoryTimestampStorage, ScnStorage}

@RunWith(classOf[JUnitRunner])
class TestTimestampActor extends FunSuite with BeforeAndAfterEach {
  var storage: ScnStorage[Timestamp] = null
  var actor: SequenceActor[Timestamp] = null

  override def beforeEach() {
    storage = new InMemoryTimestampStorage
    actor = new SequenceActor[Timestamp](storage)
    actor.start()
  }

  test("unicity of generated timestamps") {
    var results = List[Timestamp]()

    val latch = new CountDownLatch(1)

    actor.next(values => {
      results = results ::: values
      latch.countDown()
    }, 100)

    latch.await()

    assert(results === results.distinct)
    assert(results.size === 100)
  }

  test("timestamps generation with batching of 10") {
    for (i <- 0 to 999) {
      actor.next(_ => {}, 1)
    }

    val latch = new CountDownLatch(1)
    var results = List[Timestamp]()

    actor.next(values => {
      results = values
      latch.countDown()
    }, 10)

    latch.await()

    assert(results.size === 10)
  }

}
