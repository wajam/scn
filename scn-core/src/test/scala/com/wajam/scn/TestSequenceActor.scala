package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import java.util.concurrent.CountDownLatch
import storage.{ScnStorage, InMemorySequenceStorage}

@RunWith(classOf[JUnitRunner])
class TestSequenceActor extends FunSuite with BeforeAndAfterEach {
  var storage: ScnStorage[Long] = null
  var actor: SequenceActor[Long] = null

  override def beforeEach() {
    storage = new InMemorySequenceStorage
    actor = new SequenceActor[Long](storage)
    actor.start()
  }

  test("unicity of generated ids") {
    var results = List[Long]()

    for (i <- 0 to 999) {
      actor.next(values => {
        results = results ::: values
      }, Some(1))
    }

    val latch = new CountDownLatch(1)

    actor.next(values => {
      results = results ::: values
      latch.countDown()
    }, Some(100))

    latch.await()

    assert(results === results.distinct)
    assert(results.size === 1100)
  }

  test("sequence generation with batching of 10") {
    for (i <- 0 to 999) {
      actor.next(_ => {}, Some(1))
    }

    val latch = new CountDownLatch(1)
    var results = List[Long]()

    actor.next(values => {
      results = values
      latch.countDown()
    }, Some(10))

    latch.await()

    assert(results === List.range(1001, 1001 + 10), results)
    assert(results.size === 10)
  }

  test("sequence generation with batching of 100 (max_batch)") {
    val batchSize = 200

    for (i <- 0 to 999) {
      actor.next(_ => {}, Some(1))
    }

    val latch = new CountDownLatch(1)
    var results = List[Long]()

    actor.next(values => {
      results = values
      latch.countDown()
    }, Some(200))

    latch.await()

    assert(results === List.range(1001, 1001 + 100), results)
    assert(results.size === 100)
  }

}
