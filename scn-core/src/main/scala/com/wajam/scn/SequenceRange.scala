package com.wajam.scn

/**
 * Describe a sequence range
 *
 * @author : Jerome Gagnon <jerome@wajam.com>
 * @copyright Copyright (c) Wajam inc.
 *
 */
private[scn] case class SequenceRange(from: Long, to: Long) {
  val length = to - from
}
