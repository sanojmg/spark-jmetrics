package io.github.sanojmg.jmetrics.http

import cats.effect._
import org.http4s._
import org.http4s.dsl.io.{GET, _}
import org.http4s.implicits._
import org.http4s.server.blaze._
import org.http4s.implicits._
import org.http4s.client.blaze._
import org.http4s.client._
import org.http4s.client.dsl.io._
import org.http4s.MediaType
import org.http4s.Method._

import scala.concurrent.ExecutionContext.global
import cats._
import cats.effect._
import cats.implicits._
import io.circe.{Decoder, HCursor, Json}
import org.http4s.headers.{Accept, Authorization}
import org.http4s.circe._  // for implicit EntityDecoder[Json]


object HttpClient {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  def endPoint(urlStr: String): Uri = Uri.fromString(urlStr).getOrElse(uri"http://localhost:18080/api/v1")

  def req[T](uri: Uri)(implicit d: EntityDecoder[IO, T]): IO[T] = {
    val request = GET(
      uri,
      Accept(MediaType.application.json)
    )
    BlazeClientBuilder[IO](global).resource.use { client =>
      client.expect[T](request)
    }
  }
}
