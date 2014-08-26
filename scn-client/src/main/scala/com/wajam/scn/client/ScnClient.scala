package com.wajam.scn.client

import scala.concurrent.{ExecutionContext, Future}

import org.json4s.DefaultFormats

import com.wajam.asyncclient._

trait AsyncScnClient {

  def getNextSequences(sequenceName: String, length: Int = 1)(implicit ec: ExecutionContext): Future[Seq[Long]]

  def getNextTimestamps(sequenceName: String, length: Int = 1)(implicit ec: ExecutionContext): Future[Seq[Long]]

}

class ScnClientException(message: String, cause: Throwable = null) extends Exception(message, cause)

class HttpAsyncScnClient(scnServer: String, asyncClient: AsyncClient) extends AsyncScnClient {

  def getNextSequences(sequenceName: String, length: Int = 1)(implicit ec: ExecutionContext): Future[Seq[Long]] = {
    sequences(sequenceName).get(Map("length" -> length.toString)).map { response =>
      if (response.code >= 300) {
        Future.failed(throw new ScnClientException(s"SCN failure ${response.code}"))
      }
      response.value.get
    }
  }

  def getNextTimestamps(sequenceName: String, length: Int = 1)(implicit ec: ExecutionContext): Future[Seq[Long]] = {
    timestamps(sequenceName).get(Map("length" -> length.toString)).map { response =>
      if (response.code >= 300) {
        Future.failed(throw new ScnClientException(s"SCN failure ${response.code}"))
      }
      response.value.get
    }
  }

  val sequences = ScnResourceModule.SequencesResource
  val timestamps = ScnResourceModule.TimestampsResource

  object ScnResourceModule extends JsonResourceModule {
    protected def client = asyncClient

    protected val charset = "utf-8"

    protected implicit val formats = DefaultFormats

    object SequencesResource extends Resource
    with GettableResource[Seq[Long]] {

      protected def url: String = scnServer + "/sequences/"

      protected def name: String = "sequences"

      def apply(key: String) = new SequencesResource(key)
    }

    class SequencesResource(sequenceName: String) extends Resource with GettableResource[Seq[Long]] {

      protected val url = scnServer + "/sequences/" + sequenceName + "/next"

      protected val name = "sequences"

    }

    object TimestampsResource extends Resource with GettableResource[Seq[Long]] {

      protected def url: String = scnServer + "/timestamps/"

      protected def name: String = "timestamps"

      def apply(key: String) = new TimestampsResource(key)
    }

    class TimestampsResource(sequenceName: String) extends Resource
    with GettableResource[Seq[Long]] {

      protected val url = scnServer + "/timestamps/" + sequenceName + "/next"

      protected val name = "timestamps"

    }

  }

}
