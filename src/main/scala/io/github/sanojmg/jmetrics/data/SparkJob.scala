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

  val jobEntityDecoder: EntityDecoder[IO, SparkJobs] = jsonOf[IO, SparkJobs]

  def getJobs(urlStr: String, appId: String) (implicit spark: SparkSession): IO[TypedDataset[SparkJob]] = {

    val jobsUri = HttpClient.endPoint(urlStr) / "applications" / appId / "jobs"

    val sj: IO[SparkJobs] = for {
      _    <- putStrLn ("Jobs URL: " + jobsUri)
      jobs <- HttpClient.req (jobsUri)(jobEntityDecoder)
      _    <- putStrLn ("Jobs : " + jobs)
    } yield jobs

    sj map (TypedDataset.create(_))
  }
}
