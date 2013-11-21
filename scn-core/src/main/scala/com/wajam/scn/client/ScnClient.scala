package com.wajam.scn.client

import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import com.wajam.scn.{SequenceRange, Scn}
import com.wajam.nrv.utils.timestamp.{Timestamp, TimestampGenerator}
import scala.concurrent.{Future, ExecutionContext}

/**
 * Scn client that front the SCN service and batches Scn calls to avoid excessive round trips between the
 * service requesting Scn timestamps/sequence ids and the Scn server.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 *
 */
class ScnClient(scn: Scn, config: ScnClientConfig = ScnClientConfig()) extends TimestampGenerator with Instrumented {
  private val metricSequenceStackSize = metrics.gauge[Int]("scn-stack-size", "sequence")({
    sequenceStackActors.size()
  })
  private val sequenceExecutorsQueueSize = metrics.gauge("scn-executors-queue-size", "sequence") {
    sequenceExecutors.foldLeft[Int](0)((sum: Int, executor: ClientCallbackExecutor[Long]) => {
      sum + executor.queueSize
    })
  }
  private val metricTimestampStackSize = metrics.gauge[Int]("scn-stack-size", "timestamp")({
    timestampStackActors.size()
  })
  private val timestampExecutorsQueueSize = metrics.gauge("scn-executors-queue-size", "timestamp") {
    timestampExecutors.foldLeft[Int](0)((sum: Int, executor: ClientCallbackExecutor[Timestamp]) => {
      sum + executor.queueSize
    })
  }
  private val metricNbCalls = metrics.counter("scn-calls")

  private val sequenceStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[SequenceRange, Long]]
  private val sequenceExecutors = 1.to(config.numExecutor).map(i =>
    new ClientCallbackExecutor[Long]("sequences-executor-" + i, scn)).toList

  private val timestampStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[SequenceRange, Timestamp]]
  private val timestampExecutors = 1.to(config.numExecutor).map(i =>
    new ClientCallbackExecutor[Timestamp]("sequences-executor-" + i, scn)).toList

  def responseTimeout: Long = config.timeoutInMs

  /**
   * Fetch timestamps
   *
   * @param name timestamp sequence name
   * @param cb callback to call once the timestamp are assigned
   * @param nb the number of requested timestamps
   * @param token the originator message token. Callbacks with the same token are garanteed to be executed
   *              sequentialy. If sequentiality is not desired, specify -1 and the callback execution thread will be
   *              selected randomly
   */
  def fetchTimestamps(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int, token: Long) {
    metricNbCalls += 1

    val timestampActor = timestampStackActors.getOrElse(name, {
      val actor = new ScnTimestampCallQueueActor(scn, name, config.executionRateInMs, config.timeoutInMs,
        timestampExecutors)
      Option(timestampStackActors.putIfAbsent(name, actor)).getOrElse({
        // Start actor only if this is the one put in the map
        actor.start()
        actor
      })
    })

    if (nb > Timestamp.SeqPerMs) {
      cb(Nil, Some(new IllegalArgumentException(s"Requesting too many timestamps: $nb > ${Timestamp.SeqPerMs}")))
    } else {
      timestampActor ! Batched[Timestamp](ScnCallback[Timestamp](cb, nb, System.currentTimeMillis(), token,
        scn.tracer.currentContext))
    }
  }

  def fetchTimestamps(sequenceName: String, count: Int, token: Long)(implicit ec: ExecutionContext): Future[Seq[Timestamp]] = {
    import scala.concurrent.promise

    val sequencePromise = promise[Seq[Timestamp]]()

    def callback(sequences: Seq[Timestamp], error: Option[Exception]) {
      error match {
        case Some(e) => sequencePromise.failure(e)
        case None => sequencePromise.success(sequences)
      }
    }

    fetchTimestamps(sequenceName, callback, count, token)
    sequencePromise.future
  }

  /**
   * Fetch sequence ids
   *
   * @param name sequence name
   * @param cb callback to call once the sequence ids are assigned
   * @param nb the number of requested sequence ids
   * @param token the originator message token. Callbacks with the same token are garanteed to be executed
   *              sequentialy. If sequentiality is not desired, specify -1 and the callback execution thread will be
   *              selected randomly
   */
  def fetchSequenceIds(name: String, cb: (Seq[Long], Option[Exception]) => Unit, nb: Int, token: Long) {
    metricNbCalls += 1

    val sequenceActor = sequenceStackActors.getOrElse(name, {
      val actor = new ScnSequenceCallQueueActor(scn, name, config.executionRateInMs, config.timeoutInMs,
        sequenceExecutors)
      Option(sequenceStackActors.putIfAbsent(name, actor)).getOrElse({
        // Start actor only if this is the one put in the map
        actor.start()
        actor
      })
    })

    if (nb > Timestamp.SeqPerMs) {
      cb(Nil, Some(new IllegalArgumentException(s"Requesting too many sequence ids: $nb > ${Timestamp.SeqPerMs}")))
    } else {
      sequenceActor ! Batched[Long](ScnCallback[Long](cb, nb, System.currentTimeMillis(), token,
      scn.tracer.currentContext))
    }
  }

  def fetchSequenceIds(sequenceName: String, count: Int, token: Long)(implicit ec: ExecutionContext): Future[Seq[Long]] = {
    import scala.concurrent.promise

    val sequencePromise = promise[Seq[Long]]()

    def callback(sequences: Seq[Long], error: Option[Exception]) {
      error match {
        case Some(e) => sequencePromise.failure(e)
        case None => sequencePromise.success(sequences)
      }
    }

    fetchSequenceIds(sequenceName, callback, count, token)
    sequencePromise.future
  }

  def start() = {
    sequenceExecutors.foreach(_.start())
    timestampExecutors.foreach(_.start())
    this
  }
}


