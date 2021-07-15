package io.github.sanojmg.jmetrics.util

import scala.concurrent.duration._

import cats._
import cats.data._
import cats.implicits._
import org.apache.commons.io.FileUtils

case object PrintUtil {

  // =========== Time in seconds =============

  def prettyTime(seconds: Long): String = prettyTime(seconds.seconds).getOrElse("0")

  def prettyTime(seconds: Double): String = prettyTime(seconds.toInt.seconds).getOrElse("0")

  def prettyTime(duration: Duration): Option[String] = {
    val h = duration.toHours.hours
    val m = (duration - h).toMinutes.minutes
    val s = (duration - h - m).toSeconds.seconds

    Some(h).map(_.toHours).filter(_ > 0).map(_ + " hours ") |+|
      Some(m).map(_.toMinutes).filter(_ > 0).map(_ + " minutes ") |+|
      Some(s).map(_.toSeconds).filter(_ > 0).map(_ + " seconds ")
  }

  // =========== Storage/memory size in bytes =============
  def prettyBytes(bytes: Long): String = FileUtils.byteCountToDisplaySize(bytes)
}
