package com.wajam.scn.storage

/**
 * Current Time trait that return current time in millisec (UTC Timestamp)
 */
trait CurrentTime {

  def getCurrentTime = System.currentTimeMillis()

}
