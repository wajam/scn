package com.wajam.scn.storage

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestScnTimestamp extends FunSuite {

  test("sequence out of bound on new") {
    try {
      val t = new ScnTimestamp(System.currentTimeMillis(), 99999)
      fail()
    } catch {
      case ob: IndexOutOfBoundsException => // Success
      case _: Exception => fail() // Fail on other exceptions
    }
  }

}
