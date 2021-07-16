package io.github.sanojmg.jmetrics

import cats.data.ReaderT
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.implicits._

import java.nio.file.Path
import com.monovore.decline.{Command, Opts}
import io.github.sanojmg.jmetrics.config.{AppConfig, AppEnv}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.github.sanojmg.jmetrics.core.Analyzer
import org.apache.log4j.{Level, Logger}

import scala.util.Try
import cats._
import cats.data._
import cats.implicits._
import cats.data.ReaderT
import cats.effect.IO._

import scala.concurrent.ExecutionContext.global


object MainSparkJmetrics extends App {

    Logger.getRootLogger.setLevel(Level.OFF)

    val restEndpointOpt = Opts
      .option[String]("rest-endpoint", short = "r", metavar = "url",
          help = "Rest endpoint for Spark Application or History Server (Eg: http://localhost:18080/api/v1)")

    val appIdOpt = Opts.option[String]("app-id", short = "a", metavar = "id",
        help = "Spark Application Id")

    val outFileOpt = Opts.option[Path]("out-file", short = "o", metavar = "file",
        help = "Output file").orNone

    val skewThreshold = Opts.option[Double]("skew-threshold", short = "t", metavar = "ratio",
        help = "Data skew detection threshold on Max/Avg ratio").orNone

    val configOps: Opts[AppConfig] = (restEndpointOpt, appIdOpt, outFileOpt, skewThreshold)
      .mapN (AppConfig.apply)

    val command = Command(
        name = "java -jar spark-jmetrics_2.12-0.1.jar",
        header =
            s"""A tool to help optimization and troubleshooting of Apache Spark
               |jobs by analysing job metrics
               |""".stripMargin.replaceAll("\n", " ")
    ) {
        configOps
    }

    val appConf = command.parse(args, sys.env) match {
        case Left(help) =>
            System.err.println(help)
            sys.exit(1)
        case Right(conf) => conf
    }

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(s"spark-jmetrics [end-point: ${appConf.restEndpoint}, appId: ${appConf.appId}]")
      .set("spark.ui.enabled", "false")

    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .appName("spark-jmetrics")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val env = AppEnv(appConf, spark)

    import org.apache.log4j.Logger

    Logger.getRootLogger.setLevel(Level.OFF)

    implicit val cs: ContextShift[IO] = IO.contextShift(global)
    implicit val timer: Timer[IO] = IO.timer(global)

    Analyzer.run[IO]().run(env).unsafeRunSync()

     spark.stop()

    /*
    TODO:
     - Spark configs from CLI - to run in a cluster
     - Optimize this job - in memory dataset partition size, shuffle partitions
     - Write output to file
     - Cleanup logging
     */
}
