package com.wajam.scn

import collection.mutable
import com.wajam.nrv.tracing.TraceContext

/**
 * A queue to manage scn callbacks waiting for sequence/timestamps.
 *
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
case class CountedScnCallQueue[T](private val cbQueue: mutable.Queue[ScnCallback[T]], var count: Int = 0) {
  def enqueue(cb: ScnCallback[T]) {
    cbQueue.enqueue(cb)
    count += cb.nb
  }

  def dequeue(): ScnCallback[T] = {
    val cb = cbQueue.dequeue()
    count -= cb.nb
    cb
  }

  def front: Option[ScnCallback[T]] = cbQueue.headOption

  def hasMore: Boolean = !cbQueue.isEmpty
}

/*
 * Structure to hold callback related information and context.
 */
case class ScnCallback[T](callback: (Seq[T], Option[Exception]) => Unit,
                          nb: Int,
                          startTime: Long = System.currentTimeMillis(),
                          token: Long = -1,
                          context: Option[TraceContext] = None)

