package io.github.sanojmg.jmetrics.util

import cats.effect.{IO, Sync}

object CatsUtil {
  def putStrLn(value: Any) = IO(println(value))

  def readLn = IO(scala.io.StdIn.readLine())


  def putStrLn_[F[_]: Sync](value: Any) = Sync[F].delay(println(value))

  def readLn_[F[_]: Sync] = Sync[F].delay(scala.io.StdIn.readLine())
}
