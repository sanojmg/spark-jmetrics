package io.github.sanojmg.jmetrics.data

import cats.effect.{IO, LiftIO, Sync}
import cats.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.util.CatsUtil.{putStrLn, putStrLn_}
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
                             firstTaskLaunchedTime: Option[String],
                             completionTime: Option[String],
                             tasks: Seq[StageTask]
                            )

case class SparkStageAttemptTask(stageId: Int,
                                 attemptId: Int,
                                 name: String,
                                 status: String,
                                 numTasks: Int,
                                 numCompleteTasks: Int,
                                 numFailedTasks: Int,
                                 firstTaskLaunchedTime: Option[String],
                                 completionTime: Option[String],
                                 task: StageTask
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
      c.downField("firstTaskLaunchedTime").as[Option[String]],
      c.downField("completionTime").as[Option[String]],
      c.downField("tasks").as[Map[String, StageTask]].map(_.values.toSeq)
    ) mapN SparkStageAttempt.apply
  }

  def getStage[F[_]: Sync: LiftIO](env: AppEnv, stageId: Int): F[TypedDataset[SparkStageAttempt]] = {

    implicit val spark = env.sparkSession
    val stageDecoder: EntityDecoder[IO, SparkStage] = jsonOf[IO, SparkStage]

    val stageUri = HttpClient.endPoint(env.appConf.restEndpoint) / "applications" / env.appConf.appId / "stages" / stageId.toString

    val stg: F[SparkStage] = for {
      _             <- putStrLn_[F] ("Stage URL: " + stageUri)
      attempts      <- HttpClient.req (stageUri)(stageDecoder) .to[F]
      _             <- putStrLn_[F] ("Stage : " + attempts)
    } yield attempts

    stg map (TypedDataset.create(_))
  }
}
