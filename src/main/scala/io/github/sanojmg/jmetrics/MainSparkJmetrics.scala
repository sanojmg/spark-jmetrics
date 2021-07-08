package io.github.sanojmg.jmetrics

import cats.data.ReaderT
import cats.effect.IO
import cats.implicits._

import java.nio.file.Path
import com.monovore.decline.{Command, Opts}
import io.github.sanojmg.jmetrics.config.{AppConfig, AppEnv}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.github.sanojmg.jmetrics.core.Metrics.getMetrics
import org.apache.log4j.{Level, Logger}


object MainSparkJmetrics extends App {

//    val url: String = "http://localhost:18080/api/v1"
//    val appId = "local-1624798402391"

    Logger.getRootLogger.setLevel(Level.OFF)


    val restEndpointOpt = Opts
      .option[String]("rest-endpoint", short = "r", metavar = "url",
          help = "Rest endpoint for Spark Application or History Server (Eg: http://localhost:18080/api/v1)")

    val appIdOpt = Opts.option[String]("app-id", short = "a", metavar = "id", help = "Spark Application Id")

    val outFileOpt = Opts.option[Path]("out-file", short = "o", metavar = "file", help = "Output file").orNone

    val configOps: Opts[AppConfig] = (restEndpointOpt, appIdOpt, outFileOpt).mapN (AppConfig.apply)

    val commnand = Command(
        name = "java -jar spark-jmetrics_2.12-0.1.jar",
        header = "A tool to help optimization and troubleshooting of Apache Spark jobs by analysing job metrics"
    ) {
        configOps
    }

    val appConf = commnand.parse(args, sys.env) match {
        case Left(help) =>
            System.err.println(help)
            sys.exit(1)
        case Right(conf) => conf
    }

    val metrics = getMetrics()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(s"spark-jmetrics [end-point: ${appConf.restEndpoint}, appId: ${appConf.appId}]")
      .set("spark.ui.enabled", "false")

    lazy val spark = SparkSession.builder().config(sparkConf).appName("spark-jmetrics").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val env = AppEnv(appConf, spark)

//    import org.slf4j.LoggerFactory

//    val logger = LoggerFactory.getLogger(this.getClass)

    import org.apache.log4j.Logger

    Logger.getRootLogger.setLevel(Level.OFF)

    getMetrics().run(env).unsafeRunSync()

    spark.stop()
}
