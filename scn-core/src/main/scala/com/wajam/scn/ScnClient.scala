package com.wajam.scn

import scala.Option
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
  private val metricTimestampStackSize = metrics.gauge[Int]("scn-timestamps-stack-size")({
    timestampStackActors.size()
  })
  private val metricNbCalls = metrics.counter("scn-calls")

  private val sequenceStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[Long]]
  private val timestampStackActors = new ConcurrentHashMap[String, ScnCallQueueActor[Timestamp]]

  /**
   * Fetch timestamps
   *
   * @param name timestamp sequence name
   * @param cb callback to call once the timestamp are assigned
   * @param nb the number of requested timestamps
   */
  def fetchTimestamps(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int) {
    val timestampActor = timestampStackActors.getOrElse(name, {
      val actor = new ScnTimestampCallQueueActor(scn, name, config.executionRateInMs)
      actor.start()
      Option(timestampStackActors.putIfAbsent(name, actor)).getOrElse(actor)
    })
    timestampActor ! Batched[Timestamp](ScnCallback[Timestamp](cb, nb, System.currentTimeMillis(),
      scn.service.tracer.currentContext))
    metricNbCalls.count
  }

  /**
   * Fetch sequence ids
   *
   * @param name sequence name
   * @param cb callback to call once the sequence ids are assigned
   * @param nb the number of requested sequence ids
   */
  def fetchSequenceIds(name: String, cb: (Seq[Long], Option[Exception]) => Unit, nb: Int) {
    val sequenceActor = sequenceStackActors.getOrElse(name, {
      val actor = new ScnSequenceCallQueueActor(scn, name, config.executionRateInMs)
      actor.start()
      Option(sequenceStackActors.putIfAbsent(name, actor)).getOrElse(actor)
    })
    sequenceActor ! Batched[Long](ScnCallback[Long](cb, nb, System.currentTimeMillis(),
      scn.service.tracer.currentContext))
    metricNbCalls.count
  }
}


