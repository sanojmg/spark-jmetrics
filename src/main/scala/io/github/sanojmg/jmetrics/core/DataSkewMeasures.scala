package io.github.sanojmg.jmetrics.core

import io.github.sanojmg.jmetrics.data.{StageTaskStats, TaskStats}
import cats._
import cats.data._
import cats.implicits._
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.types._

import scala.concurrent.duration._


object DataSkewMeasures {

  private final val DEFAULT_SKEW_THRESHOLD: Double = 3.0

  def getStageSkewMeasure(stList: List[StageTaskStats], env: AppEnv): List[StageSkewMeasure] =
    stList
      .filter(isSkewed(_,env))
      .map(getStageSkewMeasure(_))
      .sortBy(_.skewRatio * -1)

  def isSkewed(stage: StageTaskStats, env: AppEnv) = {
    val threshold = env.appConf.skewThreshold.getOrElse(DEFAULT_SKEW_THRESHOLD)

    stage.maxDuration > threshold * stage.avgDuration ||
      stage.maxBytesRead > threshold * stage.avgBytesRead ||
      stage.maxBytesWritten > threshold * stage.avgBytesWritten ||
      stage.maxShuffleBytesRead > threshold * stage.avgShuffleBytesRead ||
      stage.maxShuffleBytesWritten > threshold * stage.avgShuffleBytesWritten
  }


  def getStageSkewMeasure(stats: StageTaskStats): StageSkewMeasure =  {
    val taskDuration = DurationAgg(
      TaskDuration,
      TimeDuration(stats.avgDuration.toInt.seconds),
      TimeDuration(stats.maxDuration.toInt.seconds)
    )
    val bytesRead = DataSizeAgg(
      BytesRead,
      StorageSize(stats.avgBytesRead.toLong),
      StorageSize(stats.maxBytesRead.toLong)
    )
    val bytesWritten = DataSizeAgg(
      BytesWritten,
      StorageSize(stats.avgBytesWritten.toLong),
      StorageSize(stats.maxBytesWritten.toLong)
    )
    val shuffleBytesRead = DataSizeAgg(
      ShuffleBytesRead,
      StorageSize(stats.avgShuffleBytesRead.toLong),
      StorageSize(stats.maxShuffleBytesRead.toLong)
    )
    val shuffleBytesWritten = DataSizeAgg(
      ShuffleBytesWritten,
      StorageSize(stats.avgShuffleBytesWritten.toLong),
      StorageSize(stats.maxShuffleBytesWritten.toLong)
    )
    val taskAgg = List(
      taskDuration,
      bytesRead,
      bytesWritten,
      shuffleBytesRead,
      shuffleBytesWritten
    )

    StageSkewMeasure(
      stats.stageId,
      "",
      stats.attemptId,
      Nil,
      TimeDuration(0.seconds),
      0,
      stats.maxDuration/stats.avgDuration,
      taskAgg
    )
  }

  /*
    TODO:
     Job Name & Duration, Stage Name & Duration
     Title - Metrics for tasks per stage: Stage with highest skew in task durations first
     Check: skew() as a measure
     --- Fix: Duration in millis
     --- Pretty time - hh:MM:ss
     Stage measures - Total number of tasks in a stage - numTasks (completed/failed/killed),
               Job Ids and names,
               shuffle read, shuffle write, input, output (memory/disk spilled)
     Top 5 Tasks by duration
     Task measures: gc time, shuffle spill - disk & memory, Avg/Max - Ratio, Percentils?
     Executor cores & memory, Driver memory
     Detailed output: Stage details
     Tasks across all stages ordered by Duration, Read/Write/Shuffle
     (Task Count, Stage, Job & Stage Averages)
   */
}
