package com.wajam.scn

import com.wajam.nrv.protocol.codec.Codec
import com.wajam.scn.protobuf.ScnProtobuf._
import scala.collection.JavaConversions._

class ScnCodec extends Codec {

  private val internal = new ScnInternalTranslator()

  def encode(entity: Any, context: Any): Array[Byte] = {
    internal.encode(entity)
  }

  def decode(data: Array[Byte], context: Any): Any = {
    internal.decode(data)
  }
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
