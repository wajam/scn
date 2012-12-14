package com.wajam.scn.client

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.{Matchers, ArgumentCaptor}
import scala.collection.JavaConversions._
import com.wajam.nrv.TimeoutException
import com.wajam.scn.{SequenceRange, MockScn}

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
    sequenceActor = new ScnSequenceCallQueueActor(mockScn, "test-timestamp", 10, 10000, mockCallbackExecutor)
    sequenceActor.start()
  }

  test("assign sequence number correctly when asked for one timestamp") {
    val expectedSequence = Seq[Long](11)
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    mockScn.nextSequenceSeq = Seq(expectedSequence)
    sequenceActor.batch(expectedCallback)
    sequenceActor.execute()

    waitForActorToProcess()

    verify(mockCallbackExecutor).executeCallback(expectedCallback, Right(expectedSequence))
  }

  test("assign sequence number correctly when asked for more than one timestamp") {
    val expectedSequence = Seq[Long](11, 12)
    mockScn.nextSequenceSeq = Seq(expectedSequence)
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 2)
    sequenceActor.batch(expectedCallback)
    sequenceActor.execute()

    waitForActorToProcess()

    verify(mockCallbackExecutor).executeCallback(expectedCallback, Right(expectedSequence))
  }

  test("assign sequence number correctly when exception occurs") {
    val expectedSequence = Seq[Long](11)
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    sequenceActor.batch(expectedCallback)

    //execute get an exception, nothing should happen
    mockScn.exception = Some(new Exception)
    sequenceActor.execute()

    waitForActorToProcess()

    verifyZeroInteractions(mockCallbackExecutor)

    //now get a response
    mockScn.exception = None
    mockScn.nextSequenceSeq = Seq(expectedSequence)
    sequenceActor.execute()

    waitForActorToProcess()

    verify(mockCallbackExecutor).executeCallback(expectedCallback, Right(expectedSequence))
  }

  test("assign sequence number correctly and in order when multiple callbacks are queue") {
    val expectedSequence = Seq[Long](11, 12)
    val expectedFirstCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)
    val expectedSecondCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1)

    sequenceActor.batch(expectedFirstCallback)
    sequenceActor.batch(expectedSecondCallback)

    mockScn.nextSequenceSeq = Seq(expectedSequence)
    val callbackCaptor: ArgumentCaptor[ScnCallback[Long]] =
      ArgumentCaptor.forClass(ScnCallback.getClass).asInstanceOf[ArgumentCaptor[ScnCallback[Long]]]
    val sequenceCaptor: ArgumentCaptor[Either[Exception, Seq[Long]]] =
      ArgumentCaptor.forClass(Seq.getClass).asInstanceOf[ArgumentCaptor[Either[Exception, Seq[Long]]]]

    sequenceActor.execute()

    waitForActorToProcess()

    verify(mockCallbackExecutor, times(2)).executeCallback(callbackCaptor.capture(), sequenceCaptor.capture())

    assert(callbackCaptor.getAllValues.toList === Seq(expectedFirstCallback, expectedSecondCallback).toList)
    assert(sequenceCaptor.getAllValues.toList === Seq(Right(Seq(11)), Right(Seq(12))).toList)
  }

  test("degenerate case when asking for 0 sequence number") {
    intercept[IllegalArgumentException] {
      sequenceActor.batch(ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {
        // This check is probably useless since this code would be called from another thread
        fail("Should not be here!")
      }, 0))
    }
  }

  test("timeout old waiting callbacks") {
    val aLongTimeAgo = 0L
    val expectedCallback = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, 1, aLongTimeAgo)
    val sequenceCaptor: ArgumentCaptor[Either[Exception, Seq[Long]]] =
      ArgumentCaptor.forClass(Either.getClass).asInstanceOf[ArgumentCaptor[Either[Exception, Seq[Long]]]]

    sequenceActor.batch(expectedCallback)
    sequenceActor.execute()
    waitForActorToProcess()

    verify(mockCallbackExecutor).executeCallback(Matchers.eq(expectedCallback), sequenceCaptor.capture())
    assert(sequenceCaptor.getValue.isLeft)
    assert(sequenceCaptor.getValue.left.get.isInstanceOf[TimeoutException])
  }

  private def waitForActorToProcess() {
    Thread.sleep(100)
  }

  test("should discard out of order ranges") {
    val callback1 = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, nb = 1, token = 1)
    val callback2 = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, nb = 3, token = 1)
    val callback3 = ScnCallback[Long]((seq: Seq[Long], ex: Option[Exception]) => {}, nb = 1, token = 1)
    sequenceActor.batch(callback1)
    sequenceActor.batch(callback2)
    sequenceActor.batch(callback3)

    mockScn.nextSequenceSeq = Seq(
      Seq(SequenceRange(11, 12)),
      Seq(SequenceRange(12, 14), SequenceRange(4, 17), SequenceRange(20, 21)),
      Seq(SequenceRange(23, 24)))

    waitForActorToProcess()

    verify(mockCallbackExecutor, times(1)).executeCallback(callback1, Right(Seq(11)))
    verify(mockCallbackExecutor, times(1)).executeCallback(callback2, Right(Seq(12, 13, 20)))
    verify(mockCallbackExecutor, times(1)).executeCallback(callback3, Right(Seq(23)))
  }

}
