package com.wajam.scn

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestSequenceActor extends FunSuite {

  val actor: SequenceActor = new SequenceActor(new InMemorySequenceStorage)
  actor.start()

  test("should generate unique sequence id") {
    for (i <- 0 to 999) {
      val value = actor.next("test%d".format(i % 10))
    }

    for (i <- 0 to 9) {
      val value = actor.next("test%d".format(i))
      assert(value == 101, value)
    }
  }
}
