package com.wajam.scn.storage

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

@RunWith(classOf[JUnitRunner])
class TestScnTimestamp extends FunSuite {

  test("sequence out of bound on new") {
    try {
      ScnTimestamp(System.currentTimeMillis(), 99999)
      fail()
    } catch {
      case ob: IndexOutOfBoundsException => // Success
      case _: Exception => fail() // Fail on other exceptions
    }
  }

  test("max should be greater than min") {
    ScnTimestamp.MAX.value should be > (ScnTimestamp.MIN.value)
  }

}
