package io.github.sanojmg.jmetrics.core

import alleycats.Pure.pureFlatMapIsMonad
import cats.implicits.catsSyntaxFlatMapOps
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => sf}
import org.apache.spark.sql._
import frameless.functions._
import frameless.functions.aggregate._
import frameless.functions.nonAggregate._
import frameless.TypedColumn._
import frameless.{TypedColumn, TypedEncoder}
import io.github.sanojmg.jmetrics.core.Metrics.{Action, printA}
import org.apache.spark.sql.expressions.Window
import io.github.sanojmg.jmetrics.data._
import frameless.FramelessSyntax
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn
import frameless.cats.implicits._
import frameless.TypedDataset
import frameless.functions._
import io.github.sanojmg.jmetrics.config.AppConfig
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.data._

import cats.implicits._
import cats.Applicative
import cats.data.ReaderT
import cats.effect.IO

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
      stages        <- stageDS.collect[Action]()
      taskDS        = stageDS.explode('tasks).as[SparkStageAttemptTask]
      tasks         <- taskDS.collect[Action]()
      _             <- printA(Console.GREEN, s"""====> Tasks: \n${tasks.mkString("\n")}""")
      _             <- stages.toList.traverse(generateMetricsForAStageAttempt(jobId, _)(env.sparkSession))
      _             <- printA(Console.YELLOW, s"----------> Done Getting Metrics for Stage id: ${stageId}")

    } yield ()
  }

  def getTaskAttr(tasks: Seq[StageTask]): Action[TypedDataset[TaskAttributesSt]] = for {
    env            <- ReaderT.ask[IO, AppEnv]
    taskDS         = TypedDataset.create(tasks)(TypedEncoder[StageTask], env.sparkSession)
    tAttrAll       = taskDS.selectMany(
                       taskDS('taskId),
                       taskDS('attempt),
                       taskDS('status),
                       taskDS('duration),
                       taskDS('resultSize),
                       taskDS('jvmGcTime),
                       taskDS('bytesRead),
                       taskDS('bytesWritten),
                       taskDS('shuffleRemoteBytesRead) + taskDS('shuffleLocalBytesRead),
                       taskDS('shuffleBytesWritten)
                     ).as[TaskAttributes]
    // Add statusOrder - this is required to de-dup tasks if there are multiple attempts
    // (eg: in case of speculative executions)
    tAttrSt        = tAttrAll.withColumn[TaskAttributesSt](
                       when(tAttrAll('status).like("SUCCESS"), lit[Int, TaskAttributes](1))
                         .otherwise(lit(2))
                     ).as[TaskAttributesSt]
  } yield tAttrSt

  // de-dup and get the tasks with status = SUCCESS or the one with latest attempt number
  // Note: Frameless doesn't have window functions, so get the dataset and use functions from vanilla spark
  def dedupTasks(taskAttr: TypedDataset[TaskAttributesSt]): Action[TypedDataset[TaskAttributesSt]] = for {
    env            <- ReaderT.ask[IO, AppEnv]
    ds             = taskAttr.dataset
    windowSpec     = Window.partitionBy(ds("taskId")).orderBy(ds("statusOrder").asc, ds("attempt").desc)
    dsRanked       = ds.withColumn("rnk", sf.row_number().over(windowSpec))
    dsLatest       = dsRanked.filter(dsRanked("rnk") === 1).drop(dsRanked("rnk"))
  } yield (dsLatest.as[TaskAttributesSt](Encoders.product[TaskAttributesSt]).typed)

  def generateStats(attrDS: TypedDataset[TaskAttributesSt]): Action[Seq[TaskStats]] = for {
    env            <- ReaderT.ask[IO, AppEnv]
    stats          <- attrDS.aggMany(
                        avg(attrDS('duration)),
                        max(attrDS('duration)),
                        avg(attrDS('bytesRead)),
                        max(attrDS('bytesRead)),
                        avg(attrDS('bytesWritten)),
                        max(attrDS('bytesWritten)),
                        avg(attrDS('shuffleBytesRead)),
                        max(attrDS('shuffleBytesRead)),
                        avg(attrDS('shuffleBytesWritten)),
                        max(attrDS('shuffleBytesWritten))
                      ).as[TaskStats].collect[Action]()
  } yield stats

  def generateMetricsForAStageAttempt(jobId: Int, stageAttempt: SparkStageAttempt)
                                     (implicit spark: SparkSession): Action[Unit] = for {
    env            <- ReaderT.ask[IO, AppEnv]
    _              <- printA(Console.YELLOW_B, s"!!!!!!> Getting Stats for Job id: ${stageAttempt.stageId}")
    tAttrSt        <- getTaskAttr(stageAttempt.tasks)
    attrDS         <- dedupTasks(tAttrSt)
    stats          <- generateStats(attrDS)
    _              <- printA(Console.RED,
                        s"""====> Task Stats for JobId: ${jobId}, StageId: ${stageAttempt.stageId},
                           | AttemptId: ${stageAttempt.attemptId}  :
                           |\n\n${stats.mkString("\n")}""".stripMargin)
  } yield ()

  def printA(color: String, value: Any): Action[Unit] = ReaderT.liftF(putStrLn(color + value + Console.RESET))

  def printA(value: Any): Action[Unit] = ReaderT.liftF(putStrLn(value))
}
