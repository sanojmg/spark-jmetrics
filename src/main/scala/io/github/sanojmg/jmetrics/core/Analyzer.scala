package io.github.sanojmg.jmetrics.core

import cats._
import cats.data._
import cats.implicits._
import cats.effect.{Concurrent, ContextShift, LiftIO, Sync}

import io.github.sanojmg.jmetrics.util.CatsUtil._
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.data._
import io.github.sanojmg.jmetrics.types.Common._

object Analyzer {

  def run[F[_]: Sync: LiftIO: Parallel: Concurrent: ContextShift](): Action[F, Unit] = for {
    env              <- Kleisli.ask[F, AppEnv]
    jobs             <- SparkJob.getJobs[F]
    stageMap         =  getStageMap(jobs)
    _                <- logA(s"======> Stage to Job Id Map: \n${stageMap.mkString("\n")}")
    stageAttempts    <- SparkStageAttempt.getStages[F](env, stageMap.keys.toList)
    stats            <- generateMetricsForAllStages[F](stageAttempts)
    skewStr          = DataSkewMeasures
                         .getStageSkewMeasure(stats, env)
                         .mkString("\n\n")
    _                <- printC(Console.RED, s"Data Skew: \n${skewStr}")
    _                <- writeToOutFile(skewStr)
    _                <- logA("End: getMetrics")

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
    _                 <- logA[F](s"=======> Running Spark job to analyse metrics...")
    stgTskAttr        <- SparkAnalyzer.explodeAsTasks[F](stages)
    stgTskAttrUnq     <- SparkAnalyzer.dedupTasks[F](stgTskAttr)(env.sparkSession)
    stats             <- SparkAnalyzer.generateStats[F](stgTskAttrUnq)
    _                 <- logA[F](s"""=======> Task Stats = \n${stats.mkString("\n")}""")
    _                 <- logA[F](Console.GREEN, s"=======> Getting Stats for All stages...Done")

  } yield stats

}
