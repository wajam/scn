package com.wajam.scn

/**
 * Description
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
private[scn] case class SequenceRange(from: Long, to: Long) {
  val range = to - from
}
