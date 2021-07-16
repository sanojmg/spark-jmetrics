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

import io.github.sanojmg.jmetrics.core.Analyzer.{Action, collect}


object FramelessAnalyzer {

  def explodeAsTasks[F[_]: Sync]
  (stages: List[SparkStageAttempt]): Action[F, TypedDataset[StageTaskAttr]] = for {

    env                <- Kleisli.ask[F, AppEnv]
    stageDS            = TypedDataset.create(stages)(TypedEncoder[SparkStageAttempt], env.sparkSession)
    stageAttrDS        = stageDS
      .select(stageDS('stageId), stageDS('attemptId), stageDS('tasks))
      .as[SparkStageAttemptAttr]
    stageAttrExplDS    = stageAttrDS.explode('tasks).as[SparkStageAttemptAttrExpl]()

    // Flatten the column 'task
    stageTaskAttr      = stageAttrExplDS.selectMany(
      stageAttrExplDS('stageId),
      stageAttrExplDS('attemptId),
      stageAttrExplDS.colMany('task, 'taskId),
      stageAttrExplDS.colMany('task, 'attempt),
      stageAttrExplDS.colMany('task, 'status),
      stageAttrExplDS.colMany('task, 'duration),
      stageAttrExplDS.colMany('task, 'resultSize),
      stageAttrExplDS.colMany('task, 'jvmGcTime),
      stageAttrExplDS.colMany('task, 'bytesRead),
      stageAttrExplDS.colMany('task, 'bytesWritten),
      stageAttrExplDS.colMany('task, 'shuffleRemoteBytesRead)
        + stageAttrExplDS.colMany('task, 'shuffleLocalBytesRead),
      stageAttrExplDS.colMany('task, 'shuffleBytesWritten)
    ).as[StageTaskAttr]

  } yield stageTaskAttr

  // de-dup and get the tasks with status = SUCCESS or the one with latest attempt number
  // Note: Frameless doesn't support window functions, so get the dataset and use functions from vanilla spark
  def dedupTasks[F[_]: Sync](taskDS: TypedDataset[StageTaskAttr])
                            (spark: SparkSession): Action[F, TypedDataset[StageTaskAttr]] = {
    import spark.implicits._
    for {
      env      <- Kleisli.ask[F, AppEnv]

      taskStDS   = taskDS
        .withColumn[StageTaskAttrSt](
          when(taskDS('status).like("SUCCESS"), lit[Int, StageTaskAttr](1))
            .otherwise(lit(2))
        ).as[StageTaskAttrSt]()

      // Caution: No type safety from here
      ds         = taskStDS.dataset
      windowSpec = Window.partitionBy(ds("stageId"), ds("attemptId"), ds("taskId"))
        .orderBy(ds("statusOrder").asc, ds("attempt").desc)
      dsRanked   = ds.withColumn("rnk", sf.row_number().over(windowSpec))
      dsLatest   = dsRanked
        .filter(dsRanked("rnk") === 1)
        .drop(dsRanked("rnk"))
        .drop(dsRanked("statusOrder"))
    } yield
      (dsLatest.as[StageTaskAttr].typed)
    //(dsLatest.as[StageTaskAttr](Encoders.product[StageTaskAttr]).typed)
  }

  def generateStats[F[_]: Sync](attrDS: TypedDataset[StageTaskAttr]): Action[F, List[StageTaskStats]] = for {
    env            <- Kleisli.ask[F, AppEnv]
    // Frameless doesn't support more than 5 aggregate functions in group by
    // So use dataset from vanilla Spark.
    ds             = attrDS.dataset

    // Caution: No type safety from here
    statDS         = ds.groupBy(ds("stageId"), ds("attemptId")).agg(
      sf.avg(ds("duration").divide(1000)).as("avgDuration"),
      sf.max(ds("duration").divide(1000).cast(IntegerType)).as("maxDuration"),
      sf.avg(ds("bytesRead")).as("avgBytesRead"),
      sf.max(ds("bytesRead")).as("maxBytesRead"),
      sf.avg(ds("bytesWritten")).as("avgBytesWritten"),
      sf.max(ds("bytesWritten")).as("maxBytesWritten"),
      sf.avg(ds("shuffleBytesRead")).as("avgShuffleBytesRead"),
      sf.max(ds("shuffleBytesRead")).as("maxShuffleBytesRead"),
      sf.avg(ds("shuffleBytesWritten")).as("avgShuffleBytesWritten"),
      sf.max(ds("shuffleBytesWritten")).as("maxShuffleBytesWritten")
    ).as[StageTaskStats](Encoders.product[StageTaskStats])

    statTDS: TypedDataset[StageTaskStats] = statDS.typed
    stats          <- collect(statTDS)
  } yield
    stats.toList
}
