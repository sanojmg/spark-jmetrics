package io.github.sanojmg.jmetrics.data

import cats.effect.{ConcurrentEffect, IO, LiftIO, Sync}
import cats.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn_
import org.apache.spark.sql.SparkSession
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

case class SparkJob(jobId: Int,
                    name: String,
                    submissionTime: String,
                    completionTime: String,
                    stageIds: Seq[Int])

object SparkJob {

  type SparkJobs = Seq[SparkJob]

  implicit val decodeJob: Decoder[SparkJob] = new Decoder[SparkJob] {
    final def apply(c: HCursor): Decoder.Result[SparkJob] = (
        c.downField("jobId").as[Int],
        c.downField("name").as[String],
        c.downField("submissionTime").as[String],
        c.downField("completionTime").as[String],
        c.downField("stageIds").as[Seq[Int]]
      ) mapN SparkJob.apply
  }

  def getJobsDS[F[_]: Sync: LiftIO](env: AppEnv): F[TypedDataset[SparkJob]] = {
    implicit val spark = env.sparkSession
    getJobs[F](env) map (TypedDataset.create(_))
  }

  def getJobs[F[_]: Sync: LiftIO](env: AppEnv): F[Seq[SparkJob]] = {

    implicit val jobEntityDecoder: EntityDecoder[IO, SparkJobs] = jsonOf[IO, SparkJobs]

    val jobsUri = HttpClient.endPoint(env.appConf.restEndpoint) / "applications" / env.appConf.appId / "jobs"

    for {
      _            <- putStrLn_[F] ("Jobs URL: " + jobsUri)
      jobs         <- HttpClient.req (jobsUri) (jobEntityDecoder) .to[F]
      _            <- putStrLn_[F] ("Jobs : " + jobs)
    } yield
      jobs
  }
}
