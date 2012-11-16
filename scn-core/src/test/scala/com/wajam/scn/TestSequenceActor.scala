package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import storage.{ScnStorage, InMemorySequenceStorage}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.{CurrentTime, ControlableCurrentTime}

@RunWith(classOf[JUnitRunner])
class TestSequenceActor extends FunSuite with BeforeAndAfter with MockitoSugar {
  var storage: ScnStorage[Long] = null
  var actor: SequenceActor[Long] = null

  before {
    storage = new InMemorySequenceStorage
    actor = new SequenceActor[Long]("test", storage)
    actor.start()
  }

  test("unicity of generated ids") {
    var results = List[Long]()

    for (i <- 0 to 999) {
      actor.next((values, e) => {
        results = results ::: values
      }, 1)
    }

    val latch = new CountDownLatch(1)

    actor.next((values, e) => {
      results = results ::: values
      latch.countDown()
    }, 100)

    latch.await()

    assert(results === results.distinct)
    assert(results.size === 1100)
  }

  test("sequence generation with batching of 10") {
    for (i <- 0 to 999) {
      actor.next((_, e) => {}, 1)
    }

    val latch = new CountDownLatch(1)
    var results = List[Long]()

    actor.next((values, e) => {
      results = values
      latch.countDown()
    }, 10)

    latch.await()

    assert(results === List.range(1001, 1001 + 10), results)
    assert(results.size === 10)
  }

  test("sequence generation with batching of 101") {
    for (i <- 0 to 999) {
      actor.next((_, e) => {}, 1)
    }

    val latch = new CountDownLatch(1)
    var results = List[Long]()

    actor.next((values, e) => {
      results = values
      latch.countDown()
    }, 101)

    latch.await()

    assert(results === List.range(1001, 1001 + 101), results)
    assert(results.size === 101)
  }

  test("error resume") {
    val expectedException = new RuntimeException()

    storage  = mock[ScnStorage[Long]]
    when(storage.next(2)).thenThrow(expectedException)
    when(storage.next(1)).thenReturn(List(1L))
    actor = new SequenceActor[Long]("test", storage)
    actor.start()

    val latch = new CountDownLatch(2)

    var error: Option[Exception] = None
    actor.next((_, e) => {
      error = e
      latch.countDown()
    }, 2)

    var results = List[Long]()
    actor.next((values, e) => {
      results = values
      latch.countDown()
    }, 1)

    latch.await(2, TimeUnit.SECONDS)

    error should be (Some(expectedException))
    results should be (List(1L))
  }

  test("drop expired message") {
    val expiration = 1000
    actor = new SequenceActor[Long]("test", storage, expiration) with CurrentTime {
      var calls = 0
      // Increase time twice than expiration on every call. Should be called twice, first when queuing message and
      // later after dequeuing to process it.
      override def currentTime = {
        calls += 1
        expiration * calls * 2L
      }
    }
    actor.start()

    val latch = new CountDownLatch(1)

    var error: Option[Exception] = None
    actor.next((_, e) => {
      error = e
      latch.countDown()
    }, 1)

    latch.await(2, TimeUnit.SECONDS)

    error should not be (None)
  }
}
