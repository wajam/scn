package com.wajam.scn

import scala.concurrent.ExecutionContext

import com.wajam.nrv.extension.json.JsonApiDSL
import com.wajam.scn.SequenceRange._

trait ScnApi extends JsonApiDSL {

  val scn: Scn

  GET("/sequences/:name/next") -> { (request, ec) =>
    implicit val iec: ExecutionContext = ec

    val sequenceName = request.paramString("name")
    val sequenceLength = request.paramOptionalInt("length").getOrElse(1)

    scn.getNextSequence(sequenceName, sequenceLength).map(seq => Some(ranges2sequence(seq)))
  }

  GET("/timestamps/:name/next") -> { (request, ec) =>
    implicit val iec: ExecutionContext = ec

    val sequenceName = request.paramString("name")
    val sequenceLength = request.paramOptionalInt("length").getOrElse(1)

    scn.getNextTimestamp(sequenceName, sequenceLength).map(seq => Some(ranges2sequence(seq)))
  }


}
