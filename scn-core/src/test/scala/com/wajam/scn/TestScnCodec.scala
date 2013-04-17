package com.wajam.scn

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class TestScnCodec extends FunSuite with ShouldMatchers {

 test("can encode/decode SequenceRange") {

   val list: List[SequenceRange] = List(SequenceRange(1, 2), SequenceRange(5, 6))

   val codec = new ScnCodec()

   val bytes = codec.encode(list)
   val list2 = codec.decode(bytes)

   list should equal(list2)
 }
}
