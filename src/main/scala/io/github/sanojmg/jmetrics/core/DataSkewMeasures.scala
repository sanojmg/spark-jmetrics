package io.github.sanojmg.jmetrics.core

import io.github.sanojmg.jmetrics.data.{StageTaskStats, TaskStats}
import io.github.sanojmg.jmetrics.util.PrintUtil.pretty
import cats._
import cats.data._
import cats.implicits._
import io.github.sanojmg.jmetrics.config.AppEnv

object DataSkewMeasures {
  private final val DEFAULT_SKEW_THRESHOLD: Double = 3.0

  def getSkew(stList: List[StageTaskStats], env: AppEnv): Option[String] =
    stList
      .filter(isSkewed(_,env))
      .sortBy(_.maxDuration * -1)
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
      getSkewStr("Duration", st.avgDuration, st.maxDuration),
      getSkewStr("Bytes Read", st.avgBytesRead, st.maxBytesRead),
      getSkewStr("Bytes Written", st.avgBytesWritten, st.maxBytesWritten),
      getSkewStr("Shuffle Bytes Read", st.avgShuffleBytesRead, st.maxShuffleBytesRead),
      getSkewStr("Shuffle Bytes Written", st.avgShuffleBytesWritten, st.maxShuffleBytesWritten)
    )
      .sequence
      .map(s => s"""\n[Stage Id: ${st.stageId}, Attempt Id: ${st.attemptId}] \n${s.mkString("\n")} """)

  def getSkewStr(skewType: String, avg: Double, max: Int): Option[String] =
    Some((skewType, avg, max))
      .map {case (skewType, avg, max)
            => f"\t${skewType}%-25s => Avg: ${pretty(avg)}%15s, Max: ${pretty(max)}%15s" }



  // Version 1 - to be removed
  def getSkew1(stList: List[TaskStats]): Option[String] =
    stList.traverse(getSkewForAStage(_)).map(_.mkString("\n"))

  // Version 1 - to be removed
  def getSkewForAStage(st: TaskStats): Option[String] =
    List(
      getSkewStr("Duration", st.avgDuration, st.maxDuration),
      getSkewStr("Bytes Read", st.avgBytesRead, st.maxBytesRead),
      getSkewStr("Bytes Written", st.avgBytesWritten, st.maxBytesWritten),
      getSkewStr("Shuffle Bytes Read", st.avgShuffleBytesRead, st.maxShuffleBytesRead),
      getSkewStr("Shuffle Bytes Written", st.avgShuffleBytesWritten, st.maxShuffleBytesWritten)
    ).sequence.map(_.mkString("\n"))

}
