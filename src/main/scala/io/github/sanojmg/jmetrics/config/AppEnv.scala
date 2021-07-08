package io.github.sanojmg.jmetrics.config

import org.apache.spark.sql.SparkSession

case class AppEnv(appConf: AppConfig, sparkSession: SparkSession)
