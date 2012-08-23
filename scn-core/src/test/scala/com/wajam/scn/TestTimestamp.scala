package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestTimestamp extends FunSuite {

  test("sequence out of bound on new") {
    try {
      val t = new Timestamp(System.currentTimeMillis(), 99999)
      fail()
    } catch {
      case ob: IndexOutOfBoundsException => // Success
      case _ : Exception => fail() // Fail on other exceptions
    }
  }

}
