package io.github.sanojmg.jmetrics.data

import cats.Parallel
import cats.data.ReaderT
import cats.effect.{Blocker, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._
import cats.effect.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.common.IOThreadPool.blocker
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.util.CatsUtil.{putStrLn, putStrLn_}
import org.apache.spark.sql.SparkSession
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf
import io.github.sanojmg.jmetrics.common.IOThreadPool._

import scala.concurrent.ExecutionContext.global
import cats.effect.IO.contextShift


case class SparkStageAttempt(stageId: Int,
                             attemptId: Int,
                             name: String,
                             status: String,
                             numTasks: Int,
                             numCompleteTasks: Int,
                             numFailedTasks: Int,
                             firstTaskLaunchedTime: Option[String],
                             completionTime: Option[String],
                             tasks: List[StageTask]
                            )

case class SparkStageAttemptAttr(stageId: Int,
                                 attemptId: Int,
                                 tasks: List[StageTask]
                                )

case class SparkStageAttemptAttrExpl(stageId: Int,
                                     attemptId: Int,
                                     task: StageTask
                                    )



//case class SparkStageAttemptTask(stageId: Int,
//                                 attemptId: Int,
//                                 name: String,
//                                 status: String,
//                                 numTasks: Int,
//                                 numCompleteTasks: Int,
//                                 numFailedTasks: Int,
//                                 firstTaskLaunchedTime: Option[String],
//                                 completionTime: Option[String],
//                                 task: StageTask
//                                )

object SparkStageAttempt {

  type SparkStage = List[SparkStageAttempt]

  // Maximum number of fibers/logical threads
  // ref: https://gitter.im/typelevel/cats-effect?at=5daf04f0ef84ab37867838f7
  val PARALLEL_N = 2

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
      c.downField("tasks").as[Map[String, StageTask]].map(_.values.toList)
    ) mapN SparkStageAttempt.apply
  }


  def getStageDS[F[_]: Sync: LiftIO: ContextShift](env: AppEnv, stageId: Int): F[TypedDataset[SparkStageAttempt]] = {
    implicit val spark = env.sparkSession
    getStage[F](env, stageId) map (TypedDataset.create(_))
  }

  def getStages[F[_]: Sync: LiftIO: Parallel: Concurrent: ContextShift](env: AppEnv, stages: List[Int]): F[List[SparkStageAttempt]] =
    stages
      .traverse(stageId => getStage[F](env, stageId)) // :: F[ListList[[SparkStageAttempt]]]
//      .parTraverseN(PARALLEL_N)(stageId => getStage[F](env, stageId)) // :: F[ListList[[SparkStageAttempt]]]
      .map(_.flatten)    // flatten = join

  def getStage[F[_]: Sync: LiftIO: ContextShift](env: AppEnv, stageId: Int): F[List[SparkStageAttempt]] = {

    val stageDecoder: EntityDecoder[IO, SparkStage] = jsonOf[IO, SparkStage]

    val stageUri = HttpClient.endPoint(env.appConf.restEndpoint) / "applications" / env.appConf.appId /
      "stages" / stageId.toString

    Blocker[F].use { bl =>
      val retVal: F[List[SparkStageAttempt]] = for {
        _             <- bl.blockOn( putStrLn_[F] ("Stage URL: " + stageUri) )
        attempts      <- bl.blockOn( HttpClient.req (stageUri)(stageDecoder) .to[F] )
        _             <- bl.blockOn( putStrLn_[F] ("Stage : " + attempts) )
      } yield
        attempts
      retVal
    }
  }
}
