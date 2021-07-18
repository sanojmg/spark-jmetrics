package io.github.sanojmg.jmetrics.types

import io.github.sanojmg.jmetrics.util.PrintUtil.{prettyBytes, prettyTime}

import scala.concurrent.duration.Duration

// Measure: Final result returned after the analysis
sealed trait Measure

// StageSkewMeasure: Stage level skew measures
case class StageSkewMeasure(
                         stageId: Int,
                         stageName: String,
                         attemptId: Int,
                         jobId: List[Int],
                         stageDuration: TimeDuration,
                         taskCount: Int,
                         skewRatio: Double,
                         taskAgg: List[TaskAggregate]
                         ) extends Measure {
  override val toString =
    f"""[Stage Id: ${stageId}, Attempt Id: ${attemptId}, Task Count: ${taskCount}, """ +
    f"""Skew Ratio: ${skewRatio}] \n${taskAgg.mkString("\n")}"""
}

// TaskAggregate: Stage level measures aggregated from task metrics
sealed trait TaskAggregate

case class DurationAgg(name: DurationAggName,
                        avg: TimeDuration,
                        max: TimeDuration
                       ) extends TaskAggregate {
  override val toString = f"\t${name}%-25s => Avg: ${avg}%-17s, Max: ${max}%-17s"
}

case class DataSizeAgg(name: DataSizeAggName,
                        avg: StorageSize,
                        max: StorageSize
                      ) extends TaskAggregate {
  override val toString = f"\t${name}%-25s => Avg: ${avg}%-17s, Max: ${max}%-17s"
}

// Aggregate based on time durations
sealed trait DurationAggName
case object TaskDuration extends DurationAggName {override val toString = "Duration (HH:mm:ss)"}

// Aggregate based on data size
sealed trait DataSizeAggName
case object BytesRead extends DataSizeAggName {override val toString = "Bytes Read"}
case object BytesWritten extends DataSizeAggName {override val toString = "Bytes Written"}
case object ShuffleBytesRead extends DataSizeAggName {override val toString = "Shuffle Bytes Read"}
case object ShuffleBytesWritten extends DataSizeAggName {override val toString = "Shuffle Bytes Written"}

case class TimeDuration(duration: Duration) {
  override val toString = prettyTime(duration).getOrElse("0")
}

case class StorageSize(size: Long) {
  override val toString = prettyBytes(size)
}
