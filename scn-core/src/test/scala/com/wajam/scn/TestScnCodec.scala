package com.wajam.scn

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.nrv.protocol.codec.{Codec, GenericJavaSerializeCodec}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestScnCodec extends FunSuite with ShouldMatchers with BeforeAndAfter {

  var codec: Codec = null

  before {
    codec = new ScnCodec()
  }

  test("can encode/decode empty") {

    val bytes = codec.encode(null)
    val data = codec.decode(bytes).asInstanceOf[AnyRef]

    data should be(null)
  }

 test("can encode/decode SequenceRange") {

   val list: List[SequenceRange] = List(SequenceRange(1, 2), SequenceRange(5, 6))

   val bytes = codec.encode(list)
   val list2 = codec.decode(bytes)

   list should equal(list2)
 }

  test("can {encode java}/{silently decode java} SequenceRange") {

    val list: List[SequenceRange] = List(SequenceRange(1, 2), SequenceRange(5, 6))

    val genericCodec = new GenericJavaSerializeCodec

    val bytes = genericCodec.encode(list)
    val list2 = codec.decode(bytes)

    list should equal(list2)
  }
}
