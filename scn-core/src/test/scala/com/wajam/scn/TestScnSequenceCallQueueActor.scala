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
class TestScnSequenceCallQueueActor extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockScn: MockScn = null
  var mockCallbackExecutor: CallbackExecutor[Long] = null
  var sequenceActor: ScnSequenceCallQueueActor = null

  before {
    mockScn = new MockScn
    mockCallbackExecutor = mock[CallbackExecutor[Long]]
    sequenceActor = new ScnSequenceCallQueueActor(mockScn, "test-timestamp", 10, Some(mockCallbackExecutor))
    sequenceActor.start()
  }

  test("assign sequence number correctly when asked for one timestamp") {
    val expectedSequence = Seq[Long](11)
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    mockScn.nextSequenceSeq = expectedSequence
    sequenceActor.batch(expectedCallback)
    sequenceActor.execute()

    waitForActorToProcess

    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedSequence)
  }

  test("assign sequence number correctly when asked for more than one timestamp") {
    val expectedSequence = Seq[Long](11, 12)
    mockScn.nextSequenceSeq = expectedSequence
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 2)
    sequenceActor.batch(expectedCallback)
    sequenceActor.execute()

    waitForActorToProcess

    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedSequence)
  }

  test("assign sequence number correctly when exception occurs") {
    val expectedSequence = Seq[Long](11)
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    sequenceActor.batch(expectedCallback)

    //execute get an exception, nothing should happen
    mockScn.exception = Some(new Exception)
    sequenceActor.execute()

    waitForActorToProcess

    verifyZeroInteractions(mockCallbackExecutor)

    //now get a response
    mockScn.exception = None
    mockScn.nextSequenceSeq = expectedSequence
    sequenceActor.execute()

    waitForActorToProcess

    verify(mockCallbackExecutor).executeCallback(expectedCallback, expectedSequence)
  }

  test("assign sequence number correctly and in order when multiple callbacks are queue") {
    val expectedSequence = Seq[Long](11, 12)
    val expectedFirstCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    val expectedSecondCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)

    sequenceActor.batch(expectedFirstCallback)
    sequenceActor.batch(expectedSecondCallback)

    mockScn.nextSequenceSeq = expectedSequence
    val callbackCaptor: ArgumentCaptor[ScnCallback[Long]] = ArgumentCaptor.forClass(ScnCallback.getClass).asInstanceOf[ArgumentCaptor[ScnCallback[Long]]]
    val sequenceCaptor: ArgumentCaptor[Seq[Long]] = ArgumentCaptor.forClass(Seq.getClass).asInstanceOf[ArgumentCaptor[Seq[Long]]]

    sequenceActor.execute()

    waitForActorToProcess

    verify(mockCallbackExecutor, times(2)).executeCallback(callbackCaptor.capture(), sequenceCaptor.capture())

    assert(callbackCaptor.getAllValues.toList === Seq(expectedFirstCallback, expectedSecondCallback).toList)
    assert(sequenceCaptor.getAllValues.toList === Seq(Seq(11), Seq(12)).toList)
  }

  test("degenerate case when asking for 0 sequence number") {
    intercept[IllegalArgumentException] {
      sequenceActor.batch(ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {
        assert(false)
      }, 0))
    }
  }

  private def waitForActorToProcess {
    Thread.sleep(100)
  }

}