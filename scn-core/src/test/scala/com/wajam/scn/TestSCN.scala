package com.wajam.scn

import org.scalatest.FunSuite
import storage.StorageType

/**
 * Description
 */
class TestSCN extends FunSuite {

  test("zookeeper storage construction (without client failure)") {
    try {
      val scn = new Scn("scn", StorageType.zookeeper)
      fail()
    } catch {
      case iae: IllegalArgumentException => // Success
      case _ => fail()
    }
  }

}
