package io.github.sanojmg.jmetrics.core


import alleycats.Pure.pureFlatMapIsMonad
import cats.implicits.catsSyntaxFlatMapOps
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, functions => sf}
import org.apache.spark.sql._
import frameless.functions._
import frameless.functions.aggregate._
import frameless.functions.nonAggregate._
import frameless.TypedColumn._
import frameless.TypedColumn
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import io.github.sanojmg.jmetrics.data._
//import frameless.syntax._
import frameless.FramelessSyntax
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn
import frameless.cats.implicits._
import frameless.TypedDataset
import frameless.functions._
import io.github.sanojmg.jmetrics.config.AppConfig
import io.github.sanojmg.jmetrics.config.AppEnv
import io.github.sanojmg.jmetrics.data.TaskAttributes

//import cats._
//import cats.effect._
//import cats.effect.implicits._
import cats.implicits._
import cats.Applicative
import cats.data.ReaderT
import cats.effect.IO

//import cats.mtl.implicits._

object Metrics {

  type Action[T] = ReaderT[IO, AppEnv, T]

  def getMetrics(): Action[Unit] = {
    for {
      _             <- printA("Start: getMetrics")

      env           <- ReaderT.ask[IO, AppEnv]
      jobDS         <- ReaderT.liftF(SparkJob.getJobs(env))
      jobs          <- jobDS.orderBy(jobDS('jobId).asc).collect[Action]()

      _             <- printA(Console.GREEN, s"""====> Jobs: \n${jobs.mkString("\n")}""")

      res           <- jobs.toList.traverse(getJobMetrics(_))  // MapM

      _             <- printA("End: getMetrics")

//      sampleJob     <- jobDS.take[Action](3)
//      jobCount      <- jobDS.count[Action]()
//      _             <- printA(s"===========> Sample Job: \n ${sampleJob.mkString("\n\n")}")
//      _             <- printA(s"===========> Job Count: ${jobCount}")



    } yield ()
  }

  def getJobMetrics(job: SparkJob): Action[Unit] = {
    for {
      _             <- printA(Console.GREEN, s"===========> Getting Metrics for Job id: ${job.jobId}")
      res           <- job.stageIds.toList.traverse(getStageMetrics(job.jobId, _))  // mapM
      _             <- printA(Console.GREEN, s"===========> Done Getting Metrics for Job id: ${job.jobId}")
    } yield ()
  }

  def getStageMetrics(jobId: Int, stageId: Int): Action[Unit] = {
    for {
      _             <- printA(Console.YELLOW, s"----------> Getting Metrics for Stage id: ${stageId}")
      env           <- ReaderT.ask[IO, AppEnv]
      stageDS       <- ReaderT.liftF(SparkStageAttempt.getStage(env, stageId))
      taskDS        = stageDS.explode('tasks).as[SparkStageAttemptTask]
      _             = taskDS.printSchema()
      tasks         <- taskDS.collect[Action]()
      _             <- printA(Console.GREEN, s"""====> Tasks: \n${tasks.mkString("\n")}""")
      _             = generateMetricsForAStage(jobId, taskDS, env.sparkSession)
      _             <- printA(Console.YELLOW, s"----------> Done Getting Metrics for Stage id: ${stageId}")

    } yield ()
  }

  def generateMetricsForAStage(jobId: Int, taskDS: TypedDataset[SparkStageAttemptTask], spark: SparkSession) = {
    // Project the required columns
    case class TaskDSProj( stageId: Int,
                           attemptId: Int,
                           task: StageTask,
                           status: String
                         )
    val taskDSProj = taskDS.select(taskDS('stageId), taskDS('attemptId), taskDS('task),
      taskDS.colMany('task, 'status)).as[TaskDSProj]

    // Add statusOrder - this is required to de-dup tasks if there are multiple attempts (eg: speculative executions)
    case class TaskDSProjSt( stageId: Int,
                             attemptId: Int,
                             task: StageTask,
                             status: String,
                             statusOrder: Int)
    val taskDSProjSt = taskDSProj.withColumn[TaskDSProjSt](
      when(taskDSProj('status).like("SUCCESS"), lit[Int, TaskDSProj](1)).
        otherwise(lit(2))
    ).as[TaskDSProjSt]

    // Flatten the column 'task
    val tAttr = taskDSProjSt.selectMany(
      taskDSProjSt('stageId),
      taskDSProjSt('attemptId),
      taskDSProjSt.colMany('task, 'taskId),
      taskDSProjSt.colMany('task, 'attempt),
      taskDSProjSt.colMany('task, 'status),
      taskDSProjSt('statusOrder),
      taskDSProjSt.colMany('task, 'duration),
      taskDSProjSt.colMany('task, 'resultSize),
      taskDSProjSt.colMany('task, 'jvmGcTime),
      taskDSProjSt.colMany('task, 'bytesRead),
      taskDSProjSt.colMany('task, 'bytesWritten),
      taskDSProjSt.colMany('task, 'shuffleRemoteBytesRead),
      taskDSProjSt.colMany('task, 'shuffleLocalBytesRead),
      taskDSProjSt.colMany('task, 'shuffleBytesWritten)
    ).as[TaskAttributes]

    // de-dup and get the tasks with status = SUCCESS or the one with latest attempt number
    // Note: Frameless doesn't have window functions, so get the dataset and use functions from vanilla spark
    import spark.implicits._
    val ds = tAttr.dataset
    val windowSpec = Window
      .partitionBy(ds("stageId"), ds("attemptId"), ds("taskId"))
      .orderBy(ds("statusOrder").asc, ds("attempt").desc)
    val dsRanked = ds.withColumn("rnk", sf.row_number().over(windowSpec))
    val dsLatest: DataFrame = dsRanked.filter(dsRanked("rnk") === 1).drop(dsRanked("rnk"))
    val attrDS: TypedDataset[TaskAttributes] = dsLatest.as[TaskAttributes].typed

    val stats = attrDS
      .groupBy(attrDS('stageId), attrDS('attemptId))
      .agg(avg(attrDS('duration)))

    // df.stat.approxQuantile("x", Array(0.5), 0.25)

    /*
     case class TaskAttributes( stageId: Int,
                               attemptId: Int,
                               taskId: Int,
                               attempt: Int,  // dedup -
                               duration: Int, // In seconds
                               resultSize: Int, // In bytes
                               jvmGcTime: Int, // In milliseconds
                               bytesRead: Int, // from source/persisted data
                               bytesWritten: Int,
                               shuffleRemoteBytesRead: Int,
                               shuffleLocalBytesRead: Int,
                               shuffleBytesWritten: Int,
                             )
     */


  }

  def printA(color: String, value: Any): Action[Unit] = ReaderT.liftF(putStrLn(color + value + Console.RESET))

  def printA(value: Any): Action[Unit] = ReaderT.liftF(putStrLn(value))
}
