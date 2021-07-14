package io.github.sanojmg.jmetrics.core

import io.github.sanojmg.jmetrics.data.{TaskStats, StageTaskStats}
import io.github.sanojmg.jmetrics.util.PrintUtil.pretty

import cats._
import cats.data._
import cats.implicits._

object DataSkewMeasures {

  def getSkew(stList: List[StageTaskStats]): Option[String] =
    stList.sortBy(_.maxDuration * -1).traverse(getSkewForAStage(_)).map(_.mkString("\n"))

  def getSkewForAStage(st: StageTaskStats): Option[String] =
    List(
      getSkewStr("Duration", st.avgDuration, st.maxDuration),
      getSkewStr("Bytes Read", st.avgBytesRead, st.maxBytesRead),
      getSkewStr("Bytes Written", st.avgBytesWritten, st.maxBytesWritten),
      getSkewStr("Shuffle Bytes Read", st.avgShuffleBytesRead, st.maxShuffleBytesRead),
      getSkewStr("Shuffle Bytes Written", st.avgShuffleBytesWritten, st.maxShuffleBytesWritten)
    )
      .sequence
      .map(s => s"""Stage Id: ${st.stageId}, Attempt Id: ${st.attemptId} ==> \n${s.mkString("\n")} """)

  def getSkewStr(skewType: String, avg: Double, max: Int): Option[String] =
    Some((skewType, avg, max))
      .filter {case (_, avg, max) => max/avg >= 0}
      .map {case (skewType, avg, max) => s"\t\t${skewType} => Avg: ${pretty(avg)}, Max: ${pretty(max)}" }

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
