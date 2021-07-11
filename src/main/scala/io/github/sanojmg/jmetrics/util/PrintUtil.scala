package io.github.sanojmg.jmetrics.util

import scala.concurrent.duration._

import cats._
import cats.data._
import cats.implicits._

case object PrintUtil {

  def pretty(seconds: Int): String = pretty(seconds.seconds).getOrElse("")

  def pretty(seconds: Double): String = pretty(seconds.toInt.seconds).getOrElse("")

  def pretty(duration: Duration): Option[String] = {
    val h = duration.toHours.hours
    val m = (duration - h).toMinutes.minutes
    val s = (duration - h - m).toSeconds.seconds

    Some(h).filter(_.toHours > 0).map(_ + "h ") |+|
      Some(m).filter(_.toMinutes > 0).map(_ + "min ") |+|
      Some(h).filter(_.toSeconds > 0).map(_ + "sec ")

  }
}
