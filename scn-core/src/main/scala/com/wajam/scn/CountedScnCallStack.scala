package com.wajam.scn

import collection.mutable

/**
 * Description
 *
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
case class CountedScnCallStack[T](private val cbStack: mutable.Stack[ScnCallback[T]], var count: Int = 0) {
  def push(cb: ScnCallback[T]) {
    cbStack.push(cb)
    count += cb.nb
  }

  def pop(): ScnCallback[T] = {
    cbStack.pop()
  }

  def top: Option[ScnCallback[T]] = cbStack.headOption

  def hasMore: Boolean = cbStack.size > 0
}

case class ScnCallback[-T](callback: (Seq[T], Option[Exception]) => Unit, nb: Int)

