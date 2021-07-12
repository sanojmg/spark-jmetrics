package io.github.sanojmg.jmetrics.core

import scala.concurrent.duration._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => sf}
import org.apache.spark.sql._
import frameless.functions._
import frameless.functions.aggregate._
import frameless.functions.nonAggregate._
import frameless.TypedColumn._
import frameless.{FramelessSyntax, SparkDelay, TypedColumn, TypedDataset, TypedEncoder}
import org.apache.spark.sql.expressions.Window
import io.github.sanojmg.jmetrics.data._
import io.github.sanojmg.jmetrics.util.CatsUtil._
import io.github.sanojmg.jmetrics.util.PrintUtil._
import frameless.cats.implicits._
import frameless.functions._
import io.github.sanojmg.jmetrics.config.AppConfig
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.data._
import cats._
import cats.data._
import cats.implicits._
import cats.data.ReaderT
import cats.effect.{IO, LiftIO, Sync}
import io.github.sanojmg.jmetrics.data.SparkJob.SparkJobs

object MetricsVer2 {
  type Action[F[_], T] = Kleisli[F, AppEnv, T]

  def getMetrics[F[_]: Sync: LiftIO](): Action[F, Unit] = for {
    _             <- printC("Start: getMetrics")
    env           <- Kleisli.ask[F, AppEnv]
    jobs          <- Kleisli.liftF(SparkJob.getJobs(env))
//    jobs          <- collect[F, SparkJob](jobDS)
    
  } yield ()


  def collect[F[_]: Sync: LiftIO, T](tds: TypedDataset[T]): Action[F, Seq[T]] =
    // Use type lambda to collect Action from TypedDataset
    // Refer: https://stackoverflow.com/a/8736360/7279631
    tds.collect[({type λ[α] = Action[F, α]})#λ]()

  def printC[F[_]: Sync: LiftIO](color: String, value: Any): Action[F, Unit] =
    Kleisli.liftF(putStrLn_(color + value + Console.RESET))

  def printC[F[_]: Sync: LiftIO](value: Any): Action[F,Unit] =
    Kleisli.liftF(putStrLn_(value))
}
