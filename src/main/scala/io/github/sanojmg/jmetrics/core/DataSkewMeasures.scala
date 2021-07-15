package io.github.sanojmg.jmetrics.core

import io.github.sanojmg.jmetrics.data.{StageTaskStats, TaskStats}
import io.github.sanojmg.jmetrics.util.PrintUtil.{prettyTime, prettyBytes}
import cats._
import cats.data._
import cats.implicits._
import io.github.sanojmg.jmetrics.config.AppEnv

object DataSkewMeasures {
  private final val DEFAULT_SKEW_THRESHOLD: Double = 3.0

  def getSkew(stList: List[StageTaskStats], env: AppEnv): Option[String] =
    stList
      .filter(isSkewed(_,env))
      .sortBy(s => s.maxDuration * -1/s.avgDuration)
      .traverse(getSkewForAStage(_))
      .map(_.mkString("\n"))

  def isSkewed(stage: StageTaskStats, env: AppEnv) = {
    val threshold = env.appConf.skewThreshold.getOrElse(DEFAULT_SKEW_THRESHOLD)

    stage.maxDuration > threshold * stage.avgDuration ||
      stage.maxBytesRead > threshold * stage.avgBytesRead ||
      stage.maxBytesWritten > threshold * stage.avgBytesWritten ||
      stage.maxShuffleBytesRead > threshold * stage.avgShuffleBytesRead ||
      stage.maxShuffleBytesWritten > threshold * stage.avgShuffleBytesWritten
  }

  def getSkewForAStage(st: StageTaskStats): Option[String] =
    List(
      getSkewStrSeconds("Duration (HH:mm:ss)", st.avgDuration, st.maxDuration),
      getSkewStrBytes("Bytes Read", st.avgBytesRead, st.maxBytesRead),
      getSkewStrBytes("Bytes Written", st.avgBytesWritten, st.maxBytesWritten),
      getSkewStrBytes("Shuffle Bytes Read", st.avgShuffleBytesRead, st.maxShuffleBytesRead),
      getSkewStrBytes("Shuffle Bytes Written", st.avgShuffleBytesWritten, st.maxShuffleBytesWritten)
    )
      .sequence
      .map(s => s"""\n[Stage Id: ${st.stageId}, Attempt Id: ${st.attemptId}] \n${s.mkString("\n")} """)

  def getSkewStrSeconds(skewType: String, avg: Double, max: Long): Option[String] =
    Some((skewType, avg, max))
      .map {case (skewType, avg, max)
            => f"\t${skewType}%-25s => Avg: ${prettyTime(avg)}%-15s, Max: ${prettyTime(max)}%-15s" }

  def getSkewStrBytes(skewType: String, avg: Double, max: Long): Option[String] =
    Some((skewType, avg, max))
      .map {case (skewType, avg, max)
      => f"\t${skewType}%-25s => Avg: ${prettyBytes(avg.toLong)}%-17s, Max: ${prettyBytes(max)}%-17s" }

  // Version 1 - to be removed
  def getSkew1(stList: List[TaskStats]): Option[String] =
    stList.traverse(getSkewForAStage(_)).map(_.mkString("\n"))

  // Version 1 - to be removed
  def getSkewForAStage(st: TaskStats): Option[String] =
    List(
      getSkewStrSeconds("Duration", st.avgDuration, st.maxDuration),
      getSkewStrBytes("Bytes Read", st.avgBytesRead, st.maxBytesRead),
      getSkewStrBytes("Bytes Written", st.avgBytesWritten, st.maxBytesWritten),
      getSkewStrBytes("Shuffle Bytes Read", st.avgShuffleBytesRead, st.maxShuffleBytesRead),
      getSkewStrBytes("Shuffle Bytes Written", st.avgShuffleBytesWritten, st.maxShuffleBytesWritten)
    ).sequence.map(_.mkString("\n"))

  /*
    TODO:
     Job Name & Duration, Stage Name & Duration
     Title - Metrics for tasks per stage: Stage with highest skew in task durations first
     Check: skew() as a measure
     Fix: Duration in millis
     Pretty time - hh:MM:ss
     Stage measures - Total number of tasks in a stage - numTasks (completed/failed/killed),
               shuffle read, shuffle write, input, output (memory/disk spilled)
     Task measures: gc time, shuffle spill - disk & memory, Avg/Max - Ratio, Percentils?
     Executor cores & memory, Driver memory
     Detailed output: Stage details
   */
}
