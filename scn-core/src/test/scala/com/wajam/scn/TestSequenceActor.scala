package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import java.util.concurrent.CountDownLatch

@RunWith(classOf[JUnitRunner])
class TestSequenceActor extends FunSuite {

  val storage = new InMemorySequenceStorage
  val actor: SequenceActor = new SequenceActor(storage)
  actor.start()

  test("should generate unique sequence id") {

    for (i <- 0 to 9999) {
      actor.next("test%d".format(i % 10), (Int) => {})
    }

    val latch = new CountDownLatch(10)
    val results = Array.fill[Int](10)(0)

    for (i <- 0 to 9) {
      actor.next("test%d".format(i), value => {
        results(i) = value
        latch.countDown()
      })
    }

    latch.await()

    results.foreach(value => {
      assert(value === 1001, value)
    })
  }

  test("should generate unique sequence with batching of 10") {
    val batchSize = 10
    for(i <- 0 to 999) {
      actor.next("testBs10", (Int) => {}, Some(batchSize))
    }

    val latch = new CountDownLatch(1)
    var result = 0

    actor.next("testBs10", value => {
      result = value
      latch.countDown()
    }, Some(batchSize))

    latch.await()

    assert(result === 1001, result)
    // Ensure batching of 10
    assert(storage.sequenceCount.getOrElse("testBs10", -1) === 1011)
  }

  test("should generate unique sequence with batching of 100") {
    val batchSize = 200

    for(i <- 0 to 999) {
      actor.next("testBs200", (Int) => {}, Some(batchSize))
    }

    val latch = new CountDownLatch(1)
    var result = 0

    actor.next("testBs200", value => {
      result = value
      latch.countDown()
    }, Some(batchSize))

    latch.await()

    assert(result === 1001, result)
    // Ensure batching of 100 even if 200 asked
    assert(storage.sequenceCount.getOrElse("testBs200", -1) === 1101)
  }
}
