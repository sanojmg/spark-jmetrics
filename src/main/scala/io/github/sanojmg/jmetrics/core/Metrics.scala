package io.github.sanojmg.jmetrics.core


import alleycats.Pure.pureFlatMapIsMonad
import cats.implicits.catsSyntaxFlatMapOps
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
import io.github.sanojmg.jmetrics.data._
import frameless.syntax._
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn
import frameless.cats.implicits._
import frameless.TypedDataset
import frameless.functions._
import io.github.sanojmg.jmetrics.config.AppConfig
import io.github.sanojmg.jmetrics.config.AppEnv

//import cats._
//import cats.effect._
//import cats.effect.implicits._
import cats.implicits._
import cats.Applicative
import cats.data.ReaderT
import cats.effect.IO

//import cats.mtl.implicits._

object Metrics {

  type Action[T] = ReaderT[IO, AppEnv, T]

  def getMetrics(): Action[Unit] = {
    for {
      _             <- printA("Start: getMetrics")

      env           <- ReaderT.ask[IO, AppEnv]
      jobDS         <- ReaderT.liftF(SparkJob.getJobs(env))
      jobs          <- jobDS.orderBy(jobDS('jobId).asc).collect[Action]()

      _             <- printA(Console.GREEN, s"""====> Jobs: \n${jobs.mkString("\n")}""")

      res           <- jobs.toList.traverse(getJobMetrics(_))  // MapM

      _             <- printA("End: getMetrics")

//      sampleJob     <- jobDS.take[Action](3)
//      jobCount      <- jobDS.count[Action]()
//      _             <- printA(s"===========> Sample Job: \n ${sampleJob.mkString("\n\n")}")
//      _             <- printA(s"===========> Job Count: ${jobCount}")



    } yield ()
  }

  def getJobMetrics(job: SparkJob): Action[Unit] = {
    for {
      _             <- printA(Console.GREEN, s"===========> Getting Metrics for Job id: ${job.jobId}")
      res           <- job.stageIds.toList.traverse(getStageMetrics(job.jobId, _))  // mapM
      _             <- printA(Console.GREEN, s"===========> Done Getting Metrics for Job id: ${job.jobId}")
    } yield ()
  }

  def getStageMetrics(jobId: Int, stageId: Int): Action[Unit] = {
    for {
      _             <- printA(Console.YELLOW, s"----------> Getting Metrics for Stage id: ${stageId}")
      env           <- ReaderT.ask[IO, AppEnv]
      stageDS       <- ReaderT.liftF(SparkStageAttempt.getStage(env, stageId))
      taskDS        = stageDS.explode('tasks).as[SparkStageAttemptTask]
      _             = taskDS.printSchema()
      tasks         <- taskDS.collect[Action]()
      _             <- printA(Console.GREEN, s"""====> Tasks: \n${tasks.mkString("\n")}""")

      _             <- printA(Console.YELLOW, s"----------> Done Getting Metrics for Stage id: ${stageId}")

    } yield ()
  }

  def getStageAttemptMetrics(jobId: Int, stageAttempt: SparkStageAttempt): Action[Unit] = {
    for {
      env           <- ReaderT.ask[IO, AppEnv]

    } yield ()
  }

  def printA(color: String, value: Any): Action[Unit] = ReaderT.liftF(putStrLn(color + value + Console.RESET))

  def printA(value: Any): Action[Unit] = ReaderT.liftF(putStrLn(value))
}
