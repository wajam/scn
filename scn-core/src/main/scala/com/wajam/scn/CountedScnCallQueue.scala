package com.wajam.scn

import collection.mutable
import com.wajam.nrv.tracing.TraceContext

/**
 * Description
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

case class ScnCallback[T](callback: (Seq[T], Option[Exception]) => Unit,
                           nb: Int,
                           startTime: Long = 0,
                           context: Option[TraceContext] = None)

