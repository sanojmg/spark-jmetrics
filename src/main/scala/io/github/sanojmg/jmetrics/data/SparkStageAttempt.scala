package io.github.sanojmg.jmetrics.data

import cats.effect.IO
import cats.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn
import org.apache.spark.sql.SparkSession
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

case class SparkStageAttempt(stageId: Int,
                             attemptId: Int,
                             name: String,
                             status: String,
                             numTasks: Int,
                             numCompleteTasks: Int,
                             numFailedTasks: Int,
                             firstTaskLaunchedTime: String,
                             completionTime: String,
                             tasks: Seq[StageTask]
                            )


object SparkStageAttempt {

  type SparkStage = Seq[SparkStageAttempt]

  implicit val decodeJob: Decoder[SparkStageAttempt] = new Decoder[SparkStageAttempt] {
    final def apply(c: HCursor): Decoder.Result[SparkStageAttempt] = (
      c.downField("stageId").as[Int],
      c.downField("attemptId").as[Int],
      c.downField("name").as[String],
      c.downField("status").as[String],
      c.downField("numTasks").as[Int],
      c.downField("numCompleteTasks").as[Int],
      c.downField("numFailedTasks").as[Int],
      c.downField("firstTaskLaunchedTime").as[String],
      c.downField("completionTime").as[String],
      c.downField("tasks").as[Map[String, StageTask]].map(_.values.toSeq)
    ) mapN SparkStageAttempt.apply
  }

  val stageDecoder: EntityDecoder[IO, SparkStage] = jsonOf[IO, SparkStage]

  def getStage(urlStr: String, appId: String, stageId: String) (implicit spark: SparkSession): IO[TypedDataset[SparkStageAttempt]] = {

    val stageUri = HttpClient.endPoint(urlStr) / "applications" / appId / "stages" / stageId

    val stg: IO[SparkStage] = for {
      _        <- putStrLn ("Stage URL: " + stageUri)
      attempts <- HttpClient.req (stageUri)(stageDecoder)
      _        <- putStrLn ("Stage : " + attempts)
    } yield attempts

    stg map (TypedDataset.create(_))
  }
}
