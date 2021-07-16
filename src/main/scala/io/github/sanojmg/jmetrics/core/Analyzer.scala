package io.github.sanojmg.jmetrics.core

import scala.concurrent.duration._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => sf}
import org.apache.spark.sql._
import frameless.functions.{lit, _}
import frameless.functions.nonAggregate.{when, _}
import frameless.TypedColumn._
import frameless.{FramelessSyntax, SparkDelay, TypedColumn, TypedDataset, TypedEncoder}
import org.apache.spark.sql.expressions.Window
import io.github.sanojmg.jmetrics.data._
import io.github.sanojmg.jmetrics.util.CatsUtil._
import io.github.sanojmg.jmetrics.util.PrintUtil._
import frameless.cats.implicits._
import frameless.functions._
import io.github.sanojmg.jmetrics.config.AppConfig
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.data._
import cats._
import cats.data._
import cats.implicits._
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.effect.{ContextShift, IO, Timer}
import cats.effect.IO.contextShift

import scala.concurrent.ExecutionContext.global
import io.github.sanojmg.jmetrics.data.SparkJob.SparkJobs
import org.apache.spark.sql.types.IntegerType

object Analyzer {

  type Action[F[_], T] = Kleisli[F, AppEnv, T]
  type JobId = Int
  type StageId = Int

  def run[F[_]: Sync: LiftIO: Parallel: Concurrent: ContextShift](): Action[F, Unit] = for {
    _                <- printC("Start: getMetrics")
    env              <- Kleisli.ask[F, AppEnv]
    // TODO - Return Kleisli from SparkJob.getJobs and remove passing env around
    jobs             <- Kleisli.liftF(SparkJob.getJobs(env))
    _                <- printC("================>Start: getStageMap...")
    stageMap         =  getStageMap(jobs)
    _                <- printC(s"================>stageMap: \n${stageMap.mkString("\n")}")
    // TODO - Change below function to return Kleisli instead of F
    stageAttempts    <- Kleisli.liftF(SparkStageAttempt.getStages[F](env, stageMap.keys.toList))
    //_                <- printC(s"================>stageAttempts: \n${stageAttempts.mkString("\n")}")
    stats            <- generateMetricsForAllStages[F](stageAttempts)
    skewStr          = DataSkewMeasures
                         .getStageSkewMeasure(stats, env)
                         .mkString("\n\n")
    _                <- printC(Console.RED, s"Data Skew: \n${skewStr}")
    _                <- writeToOutFile(skewStr)
    _                <- printC("End: getMetrics")

  } yield ()

  def getStageMap(jobs: List[SparkJob]): Map[StageId, List[JobId]] = {
    val pairs: List[(StageId, JobId)] =
      jobs >>= (j => j.stageIds.map ((_, j.jobId)))

    pairs
      .groupBy(_._1)  // Map[(StageId, List[(StageId, JobId)])]
      .mapValues(v => v.map(_._2))  // Map[(StageId, List[JobId])]
  }

  def generateMetricsForAllStages[F[_]: Sync: LiftIO: Parallel: Concurrent: ContextShift]
              (stages: List[SparkStageAttempt]): Action[F, List[StageTaskStats]] = for {
    env               <- Kleisli.ask[F, AppEnv]
    _                 <- printC[F](Console.BOLD, s"=======> Running Spark job to analyse metrics...")
    stgTskAttr        <- FramelessAnalyzer.explodeAsTasks[F](stages)
//    taskCount         <- count(stgTskAttr)
//    _                 <- printC[F](Console.YELLOW_B, s"=======> count(stageTaskAttr) = ${taskCount}")
    stgTskAttrUnq     <- FramelessAnalyzer.dedupTasks[F](stgTskAttr)(env.sparkSession)
//    taskUnqCount      <- count(stgTskAttrUnq)
//    _                 <- printC[F](Console.YELLOW_B, s"=======> count(stgTskAttrUnq) = ${taskUnqCount}")
    stats             <- FramelessAnalyzer.generateStats[F](stgTskAttrUnq)
    _                 <- printC[F](Console.CYAN, s"""=======> Task Stats = \n${stats.mkString("\n")}""")
    _                 <- printC[F](Console.GREEN, s"=======> Getting Stats for All stages...Done")

  } yield stats



  def writeToOutFile[F[_]: Sync: Concurrent](data: String): Action[F, Unit] = for {
    env              <- Kleisli.ask[F, AppEnv]
    outFile          = env.appConf.outFile
    _                <- Kleisli.liftF(outFile.traverse(writeFile[F](_,data)))
    _                <- printC(Console.GREEN, s"Output has been written to " +
                            s"${outFile.getOrElse("--")}")
  } yield ()

  def collect[F[_]: Sync, T](tds: TypedDataset[T]): Action[F, Seq[T]] =
    // Use type lambda to collect Action from TypedDataset
    // Refer: https://stackoverflow.com/a/8736360/7279631
    tds.collect[({type λ[α] = Action[F, α]})#λ]()

  def count[F[_]: Sync, T](tds: TypedDataset[T]): Action[F, Long] =
    tds.count[({type λ[α] = Action[F, α]})#λ]()

  def printC[F[_]: Sync](color: String, value: Any): Action[F, Unit] =
    Kleisli.liftF(putStrLn_(color + value + Console.RESET))

  def printC[F[_]: Sync](value: Any): Action[F,Unit] =
    Kleisli.liftF(putStrLn_(value))
}
