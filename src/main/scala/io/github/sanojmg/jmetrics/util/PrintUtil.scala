package io.github.sanojmg.jmetrics.util

import scala.concurrent.duration._

import cats._
import cats.data._
import cats.implicits._

case object PrintUtil {

  def pretty(seconds: Int): String = pretty(seconds.seconds).getOrElse("0")

  def pretty(seconds: Double): String = pretty(seconds.toInt.seconds).getOrElse("0")

  def pretty(duration: Duration): Option[String] = {
    val h = duration.toHours.hours
    val m = (duration - h).toMinutes.minutes
    val s = (duration - h - m).toSeconds.seconds

    Some(h).map(_.toHours).filter(_ > 0).map(_ + " hr ") |+|
      Some(m).map(_.toMinutes).filter(_ > 0).map(_ + " min ") |+|
      Some(s).map(_.toSeconds).filter(_ > 0).map(_ + " sec ")
  }
}
