package com.wajam.scn

import com.wajam.nrv.protocol.codec.{GenericJavaSerializeCodec, Codec}
import com.wajam.scn.protobuf.ScnProtobuf._
import scala.collection.JavaConversions._
import java.nio.ByteBuffer

class ScnCodec extends Codec {

  private val internal = new ScnInternalTranslator()

  def encode(entity: Any, context: Any): Array[Byte] = {
    internal.encode(entity)
  }

  def decode(data: Array[Byte], context: Any): Any = {

    // If java serialized, decode with java, else decode with mryCodec.
    val magicShort: Int = ByteBuffer.wrap(data, 0, 2).getShort

    if (magicShort == ScnCodec.JavaSerializeMagicShort)
      ScnCodec.genericCodec.decode(data)
    else
      internal.decode(data)
  }
}

object ScnCodec {
  // Source: http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
  private val JavaSerializeMagicShort : Short = (0xACED).toShort

  private val genericCodec = new GenericJavaSerializeCodec
}

private[scn] class ScnInternalTranslator {

  def encode(entity: Any): Array[Byte] = {

    val transport = PTransport.newBuilder()

    entity match {
      case list: List[SequenceRange] if list.forall(_.isInstanceOf[SequenceRange]) =>
        list.foreach((psr) => transport.addSequenceRanges(encodeSequenceRange(psr)))

      case _ => throw new RuntimeException("Unsupported entity type: " + entity.getClass)
    }

    transport.build().toByteArray
  }

  def decode(data: Array[Byte]): Any = {

    val transport = PTransport.parseFrom(data)

    transport.getType match {
      case PTransport.Type.ListSequenceRange =>
        transport.getSequenceRangesList.map(decodeSequenceRange(_)).toList

      case _ => throw new RuntimeException("Unsupported entity type: " + transport.getType)
    }
  }

  private def encodeSequenceRange(sr: SequenceRange): PSequenceRange.Builder = {

    val psr = PSequenceRange.newBuilder()

    psr.setFrom(sr.from)
    psr.setTo(sr.to)
  }

  private def decodeSequenceRange(psr: PSequenceRange): SequenceRange = {

    new SequenceRange(psr.getFrom, psr.getTo)
  }
}
