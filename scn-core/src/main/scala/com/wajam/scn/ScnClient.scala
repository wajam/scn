package com.wajam.scn

import com.yammer.metrics.scala.Instrumented
import java.util.concurrent._
import scala.collection.JavaConversions._

/**
 * Scn client that front the SCN service and batches Scn calls to avoid excessive round trips between the
 * service requesting Scn timestamps/sequence ids and the Scn server.
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
class ScnClient(scn: Scn, config: ScnClientConfig = ScnClientConfig()) extends Instrumented {
  private val metricSequenceStackSize = metrics.gauge[Int]("scn-sequences-stack-size")({
    sequenceStackActors.size()
  })
  private val sequenceExecutorsQueueSize = metrics.gauge("scn-sequences-executors-queue-size") {
    sequenceExecutors.foldLeft[Int](0)((sum: Int, executor: ClientCallbackExecutor[Long]) => {
      sum + executor.queueSize
    })
  }
  private val metricTimestampStackSize = metrics.gauge[Int]("scn-timestamps-stack-size")({
    timestampStackActors.size()
  })
  private val timestampExecutorsQueueSize = metrics.gauge("scn-timestamp-executors-queue-size") {
    timestampExecutors.foldLeft[Int](0)((sum: Int, executor: ClientCallbackExecutor[Timestamp]) => {
      sum + executor.queueSize
    })
  }
  private val metricNbCalls = metrics.counter("scn-calls")

  private val sequenceStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[Long]]
  private val sequenceExecutors = 1.to(config.numExecutor).map(i =>
    new ClientCallbackExecutor[Long]("sequences-executor-" + i, scn)).toList

  private val timestampStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[Timestamp]]
  private val timestampExecutors = 1.to(config.numExecutor).map(i =>
    new ClientCallbackExecutor[Timestamp]("sequences-executor-" + i, scn)).toList

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
    val timestampActor = timestampStackActors.getOrElse(name, {
      val actor = new ScnTimestampCallQueueActor(scn, name, config.executionRateInMs, config.timeoutInMs,
        timestampExecutors)
      actor.start()
      Option(timestampStackActors.putIfAbsent(name, actor)).getOrElse(actor)
    })
    timestampActor ! Batched[Timestamp](ScnCallback[Timestamp](cb, nb, System.currentTimeMillis(), token,
      scn.service.tracer.currentContext))
    metricNbCalls.count
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
    val sequenceActor = sequenceStackActors.getOrElse(name, {
      val actor = new ScnSequenceCallQueueActor(scn, name, config.executionRateInMs, config.timeoutInMs,
        sequenceExecutors)
      actor.start()
      Option(sequenceStackActors.putIfAbsent(name, actor)).getOrElse(actor)
    })
    sequenceActor ! Batched[Long](ScnCallback[Long](cb, nb, System.currentTimeMillis(), token,
      scn.service.tracer.currentContext))
    metricNbCalls.count
  }

  def start() = {
    sequenceExecutors.foreach(_.start())
    timestampExecutors.foreach(_.start())
    this
  }
}


