package io.github.sanojmg.jmetrics.core


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import frameless.functions.aggregate._
import io.github.sanojmg.jmetrics.data.SparkJob
import frameless.syntax._
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn

import frameless.cats.implicits._
import frameless.TypedDataset

//import cats._
//import cats.effect._
//import cats.effect.implicits._
//import cats.implicits._
import cats.data.ReaderT
import cats.effect.IO

//import cats.mtl.implicits._

object Metrics {

  type Action[T] = ReaderT[IO, SparkSession, T]

  def getMetrics(url: String, appId: String): Action[(Seq[SparkJob], Long)] = {
    for {
      _             <- ReaderT.liftF (putStrLn("Starting Json download"))
      session       <- ReaderT.ask[IO, SparkSession]

      sampleJobTDS  <- ReaderT.liftF(SparkJob.getJobs(url, appId)(session))

      sampleJob     <- sampleJobTDS.take[Action](3)
      jobCount      <- sampleJobTDS.count[Action]()
      _             <- ReaderT.liftF (putStrLn("===========> Sample Job: \n" + sampleJob.mkString("\n\n")))
      _             <- ReaderT.liftF (putStrLn("===========> Job Count: " + jobCount))
    } yield (sampleJob, jobCount)
  }
}
