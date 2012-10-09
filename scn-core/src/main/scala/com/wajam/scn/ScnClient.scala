package com.wajam.scn

import scala.Option
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent._
import scala.collection.JavaConversions._

/**
 * Description
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

  def getNextTimestamp(name: String, cb: (Seq[Timestamp], Option[Exception]) => Unit, nb: Int) {
    val timestampActor = timestampStackActors.getOrElse(name, {
      val actor = new ScnTimestampCallQueueActor(scn, name, config.executionRateInMs)
      actor.start()
      Option(timestampStackActors.putIfAbsent(name, actor)).getOrElse(actor)
    })
    timestampActor ! Batched[Timestamp](ScnCallback[Timestamp](cb, nb, System.currentTimeMillis(),
      scn.service.tracer.currentContext))
    metricNbCalls.count
  }

  def getNextSequence(name: String, cb: (Seq[Long], Option[Exception]) => Unit, nb: Int) {
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


