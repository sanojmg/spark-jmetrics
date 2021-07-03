package io.github.sanojmg.jmetrics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import io.github.sanojmg.jmetrics.core.Metrics.getMetrics

object MainSparkJmetrics extends App {

  val url: String = "http://localhost:18080/api/v1"
  val appId = "local-1624798402391"

  val metrics= getMetrics(url, appId)

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(s"spark-jmetrics [uri: ${url}, appId: ${appId}]")
    .set("spark.ui.enabled", "false")

  implicit val spark = SparkSession.builder().config(conf).appName("spark-jmetrics").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  metrics.run(spark).unsafeRunSync()

  spark.stop()
}
