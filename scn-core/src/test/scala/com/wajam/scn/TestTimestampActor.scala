package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import storage.{ScnTimestamp, InMemoryTimestampStorage, ScnStorage}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.CurrentTime

@RunWith(classOf[JUnitRunner])
class TestTimestampActor extends FunSuite with BeforeAndAfter with MockitoSugar {
  var storage: ScnStorage[SequenceRange] = null
  var actor: SequenceActor[SequenceRange] = null

  before {
    storage = new InMemoryTimestampStorage
    actor = new SequenceActor[SequenceRange]("test", storage)
    actor.start()
  }

  test("unicity of generated timestamps") {
    var results = List[Timestamp]()

    val latch = new CountDownLatch(1)

    actor.next((values, e) => {
      results = results ::: ScnTimestamp.ranges2timestamps(values)
      latch.countDown()
    }, 100)

    latch.await()

    assert(results === results.distinct)
    assert(results.size === 100)
  }

  test("timestamps generation with batching of 10") {
    for (i <- 0 to 999) {
      actor.next((_, e) => {}, 1)
    }

    val latch = new CountDownLatch(1)
    var results = List[Timestamp]()

    actor.next((values, e) => {
      results = ScnTimestamp.ranges2timestamps(values)
      latch.countDown()
    }, 10)

    latch.await()

    assert(results.size === 10)
  }

  test("error resume") {
    val expectedException = new RuntimeException()

    storage  = mock[ScnStorage[SequenceRange]]
    when(storage.next(2)).thenThrow(expectedException)
    when(storage.next(1)).thenReturn(List(SequenceRange(1, 2)))
    actor = new SequenceActor[SequenceRange]("test", storage)
    actor.start()

    val latch = new CountDownLatch(2)

    var error: Option[Exception] = None
    actor.next((_, e) => {
      error = e
      latch.countDown()
    }, 2)

    var results = List[Timestamp]()
    actor.next((values, e) => {
      results = ScnTimestamp.ranges2timestamps(values)
      latch.countDown()
    }, 1)

    latch.await(2, TimeUnit.SECONDS)

    error should be (Some(expectedException))
    results should be (List(Timestamp(1L)))
  }

  test("drop expired message") {
    val expiration = 1000
    actor = new SequenceActor[SequenceRange]("test", storage, expiration) with CurrentTime {
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
