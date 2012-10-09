package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import scala.collection.JavaConversions._

/**
 * 
 */
@RunWith(classOf[JUnitRunner])
class TestScnTimestampCallQueueActor extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockScn: MockScn = null
  var mockCallbackExecutor: CallbackExecutor[Timestamp] = null
  var timestampActor: ScnTimestampCallQueueActor = null

  before {
    mockScn = new MockScn
    mockCallbackExecutor = mock[CallbackExecutor[Timestamp]]
    timestampActor = new ScnTimestampCallQueueActor(mockScn, "test-timestamp", 10, Some(mockCallbackExecutor))
    timestampActor.start()
  }

  test("assign timestamp correctly when asked for one timestamp") {
    val expectedTimestamp = Seq[Timestamp](Timestamp(11))
    val expectedCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)
    mockScn.nextTimestampSeq = expectedTimestamp

    timestampActor.batch(expectedCallback)
    timestampActor.execute()
    waitForActorToProcess
    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedTimestamp)
  }

  test("assign timestamp correctly when asked for more than one timestamp") {
    val expectedTimestamp = Seq[Timestamp](Timestamp(11), Timestamp(12))
    val expectedCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 2)
    mockScn.nextTimestampSeq = expectedTimestamp
    timestampActor.batch(expectedCallback)
    timestampActor.execute()
    waitForActorToProcess
    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedTimestamp)
  }

  test("assign timestamp correctly when exception occurs") {
    val expectedTimestamp = Seq[Timestamp](Timestamp(11))
    val expectedCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)

    timestampActor.batch(expectedCallback)

    //execute get an exception, nothing should happen
    mockScn.exception = Some(new Exception)
    timestampActor.execute()
    waitForActorToProcess
    verifyZeroInteractions(mockCallbackExecutor)

    //now get a response
    mockScn.exception = None
    mockScn.nextTimestampSeq = expectedTimestamp
    timestampActor.execute()
    waitForActorToProcess
    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedTimestamp)
  }

  test("assign timestamp correctly and in order when multiple callbacks are queue") {
    val expectedTimestamp = Seq[Timestamp](Timestamp(11), Timestamp(12))
    val expectedFirstCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)
    val expectedSecondCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)

    timestampActor.batch(expectedFirstCallback)
    timestampActor.batch(expectedSecondCallback)

    mockScn.nextTimestampSeq = expectedTimestamp
    val callbackCaptor: ArgumentCaptor[ScnCallback[Timestamp]] = ArgumentCaptor.forClass(ScnCallback.getClass).asInstanceOf[ArgumentCaptor[ScnCallback[Timestamp]]]
    val sequenceCaptor: ArgumentCaptor[Seq[Timestamp]] = ArgumentCaptor.forClass(Seq.getClass).asInstanceOf[ArgumentCaptor[Seq[Timestamp]]]
    timestampActor.execute()
    waitForActorToProcess

    verify(mockCallbackExecutor, times(2)).executeCallback(callbackCaptor.capture(), sequenceCaptor.capture())
    assert(callbackCaptor.getAllValues.toList === Seq(expectedFirstCallback, expectedSecondCallback).toList)
    assert(sequenceCaptor.getAllValues.toList === Seq(Seq(Timestamp(11)), Seq(Timestamp(12))).toList)
  }

  test("do not assign timestamp if timestamp are not in increasing order") {
    val expectedTimestamp = Seq[Timestamp](Timestamp(11))
    val expectedFirstCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)
    val expectedSecondCallback = ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {}, 1)
    timestampActor.batch(expectedFirstCallback)
    timestampActor.batch(expectedSecondCallback)

    mockScn.nextTimestampSeq = expectedTimestamp
    val callbackCaptor1: ArgumentCaptor[ScnCallback[Timestamp]] = ArgumentCaptor.forClass(ScnCallback.getClass).asInstanceOf[ArgumentCaptor[ScnCallback[Timestamp]]]
    val sequenceCaptor1: ArgumentCaptor[Seq[Timestamp]] = ArgumentCaptor.forClass(Seq.getClass).asInstanceOf[ArgumentCaptor[Seq[Timestamp]]]
    timestampActor.execute()
    timestampActor.execute() //simulate receive the same sequence twice
    waitForActorToProcess

    verify(mockCallbackExecutor, times(1)).executeCallback(callbackCaptor1.capture(), sequenceCaptor1.capture())
    assert(callbackCaptor1.getAllValues.toList === Seq(expectedFirstCallback).toList)
    assert(sequenceCaptor1.getAllValues.toList === Seq(Seq(Timestamp(11))).toList)


    reset(mockCallbackExecutor)
    val expectedTimestamp2 = Seq[Timestamp](Timestamp(12))
    mockScn.nextTimestampSeq = expectedTimestamp2
    val callbackCaptor2: ArgumentCaptor[ScnCallback[Timestamp]] = ArgumentCaptor.forClass(ScnCallback.getClass).asInstanceOf[ArgumentCaptor[ScnCallback[Timestamp]]]
    val sequenceCaptor2: ArgumentCaptor[Seq[Timestamp]] = ArgumentCaptor.forClass(Seq.getClass).asInstanceOf[ArgumentCaptor[Seq[Timestamp]]]
    timestampActor.execute()
    waitForActorToProcess

    verify(mockCallbackExecutor, times(1)).executeCallback(callbackCaptor2.capture(), sequenceCaptor2.capture())
    assert(callbackCaptor2.getAllValues.toList === Seq(expectedSecondCallback).toList)
    assert(sequenceCaptor2.getAllValues.toList === Seq(Seq(Timestamp(12))).toList)

  }

  test("degenerate case when asking for 0 timestamp") {
    intercept[IllegalArgumentException] {
      timestampActor.batch(ScnCallback[Timestamp]((seq: Seq[Timestamp], ex: Option[Exception]) => {
        assert(false)
      }, 0))
    }
  }

  private def waitForActorToProcess {
    Thread.sleep(100)
  }
}