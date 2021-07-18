package io.github.sanojmg.jmetrics.data

import cats.data.Kleisli
import cats.effect.{ConcurrentEffect, IO, LiftIO, Sync}
import cats.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.types.Common.Action
import io.github.sanojmg.jmetrics.util.CatsUtil.{logA, putStrLn_}
import org.apache.spark.sql.SparkSession
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

case class SparkJob(jobId: Int,
                    name: String,
                    submissionTime: String,
                    completionTime: String,
                    stageIds: List[Int])

object SparkJob {

  implicit val decodeJob: Decoder[SparkJob] = new Decoder[SparkJob] {
    final def apply(c: HCursor): Decoder.Result[SparkJob] = (
        c.downField("jobId").as[Int],
        c.downField("name").as[String],
        c.downField("submissionTime").as[String],
        c.downField("completionTime").as[String],
        c.downField("stageIds").as[List[Int]]
      ) mapN SparkJob.apply
  }

  def getJobsDS[F[_]: Sync: LiftIO](env: AppEnv): Action[F, TypedDataset[SparkJob]] = {
    implicit val spark = env.sparkSession
    getJobs[F] map (TypedDataset.create(_))
  }

  implicit val jobEntityDecoder: EntityDecoder[IO, List[SparkJob]] = jsonOf[IO, List[SparkJob]]

  def getJobs[F[_]: Sync: LiftIO](): Action[F, List[SparkJob]] = for {
      env          <- Kleisli.ask[F, AppEnv]
      jobsUri      = HttpClient.endPoint(env.appConf.restEndpoint) / "applications" / env.appConf.appId / "jobs"
      _            <- logA[F] ("Jobs URL: " + jobsUri)
      jobs         <- Kleisli.liftF(HttpClient.req(jobsUri).to[F])
      _            <- logA[F] ("Jobs : " + jobs)
    } yield
      jobs

}

