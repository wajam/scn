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
case class CountedScnCallStack(private val cbStack: mutable.Stack[ScnCallback], cbType: ScnCallbackType.Value, var count: Int = 0) {
  def push(cb: ScnCallback) {
    cbStack.push(cb)
    count += cb.nb
  }

  def pop(): ScnCallback = {
    val seq = cbStack.pop()
    count -= seq.nb
    seq
  }

  val top: ScnCallback = cbStack.top
}

case class ScnCallback(callback: (Any, Option[Exception]) => Unit, nb: Int)

object ScnCallbackType extends Enumeration {
  type SequenceType = Value
  val sequence, timestamp = Value
}
