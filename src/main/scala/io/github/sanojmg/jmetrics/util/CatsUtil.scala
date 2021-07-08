package io.github.sanojmg.jmetrics.util

import cats.effect.IO

object CatsUtil {
  def putStrLn(value: Any) = IO(println(value))

  val readLn = IO(scala.io.StdIn.readLine())
}
