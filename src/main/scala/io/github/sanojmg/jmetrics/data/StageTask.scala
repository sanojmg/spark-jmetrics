package io.github.sanojmg.jmetrics.data

import cats.effect.IO
import cats.implicits._
import frameless.TypedDataset
import io.circe.{Decoder, HCursor}
import io.github.sanojmg.jmetrics.http.HttpClient
import io.github.sanojmg.jmetrics.util.CatsUtil.putStrLn
import org.apache.spark.sql.SparkSession
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

import io.circe.generic.auto._
import io.circe.syntax._

case class StageTask(taskId: Int,
                     attempt: Int,
                     launchTime: String,
                     duration: Int,
                     executorId: String,
                     host: String,
                     status: String,
                     executorRunTime: Int,
                     resultSize: Int,
                     jvmGcTime: Int,
                     peakExecutionMemory: Int,
                     bytesRead: Int,
                     recordsRead: Int,
                     bytesWritten: Int,
                     recordsWritten: Int,
                     shuffleRemoteBytesRead: Int,
                     shuffleLocalBytesRead: Int,
                     shuffleRecordsRead: Int,
                     shuffleBytesWritten: Int,
                     shuffleWriteTime: Int,
                     shuffleRecordsWritten: Int)

case class TaskAttributes( taskId: Int,
                           attempt: Int,  // dedup
                           status: String, // Eg: SUCCESS
                           duration: Int, // In seconds
                           resultSize: Int, // In bytes
                           jvmGcTime: Int, // In milliseconds
                           bytesRead: Int, // from source/persisted data
                           bytesWritten: Int,
                           shuffleBytesRead: Int,
                           shuffleBytesWritten: Int)

case class StageTaskAttr( stageId: Int,
                          attemptId: Int,
                          taskId: Int,
                          attempt: Int,  // dedup
                          status: String, // Eg: SUCCESS
                          duration: Int, // In seconds
                          resultSize: Int, // In bytes
                          jvmGcTime: Int, // In milliseconds
                          bytesRead: Int, // from source/persisted data
                          bytesWritten: Int,
                          shuffleBytesRead: Int,
                          shuffleBytesWritten: Int)

case class StageTaskAttrSt( stageId: Int,
                            attemptId: Int,
                            taskId: Int,
                            attempt: Int,  // dedup
                            status: String, // Eg: SUCCESS
                            duration: Int, // In seconds
                            resultSize: Int, // In bytes
                            jvmGcTime: Int, // In milliseconds
                            bytesRead: Int, // from source/persisted data
                            bytesWritten: Int,
                            shuffleBytesRead: Int,
                            shuffleBytesWritten: Int,
                            statusOrder: Int)

case class TaskAttributesSt( taskId: Int,
                             attempt: Int,  // dedup
                             status: String, // Eg: SUCCESS
                             duration: Int, // In seconds
                             resultSize: Int, // In bytes
                             jvmGcTime: Int, // In milliseconds
                             bytesRead: Int, // from source/persisted data
                             bytesWritten: Int,
                             shuffleBytesRead: Int,
                             shuffleBytesWritten: Int,
                             statusOrder: Int)

//case class StageTaskAttributes( taskId: Int,
//                           attempt: Int,  // dedup
//                           status: String, // Eg: SUCCESS
//                           duration: Int, // In seconds
//                           resultSize: Int, // In bytes
//                           jvmGcTime: Int, // In milliseconds
//                           bytesRead: Int, // from source/persisted data
//                           bytesWritten: Int,
//                           shuffleBytesRead: Int,
//                           shuffleBytesWritten: Int)


case class TaskDSProj( stageId: Int,
                       attemptId: Int,
                       task: StageTask,
                       status: String
                     )

case class TaskStats(avgDuration: Double,
                     maxDuration: Int,
                     avgBytesRead: Double,
                     maxBytesRead: Int,
                     avgBytesWritten: Double,
                     maxBytesWritten: Int,
                     avgShuffleBytesRead: Double,
                     maxShuffleBytesRead: Int,
                     avgShuffleBytesWritten: Double,
                     maxShuffleBytesWritten: Int
                    )

case class StageTaskStats(stageId: Int,
                          attemptId: Int,
                          avgDuration: Double,
                          maxDuration: Int,
                          avgBytesRead: Double,
                          maxBytesRead: Int,
                          avgBytesWritten: Double,
                          maxBytesWritten: Int,
                          avgShuffleBytesRead: Double,
                          maxShuffleBytesRead: Int,
                          avgShuffleBytesWritten: Double,
                          maxShuffleBytesWritten: Int
                         ) {
  override def toString() = this.asJson.noSpaces
}

object StageTask {

  type StageTasks = List[StageTask]

  implicit val decodeJob: Decoder[StageTask] = new Decoder[StageTask] {
    final def apply(c: HCursor): Decoder.Result[StageTask] = (
      c.downField("taskId").as[Int],
      c.downField("attempt").as[Int],
      c.downField("launchTime").as[String],
      c.downField("duration").as[Int],
      c.downField("executorId").as[String],
      c.downField("host").as[String],
      c.downField("status").as[String],
      c.downField("taskMetrics").downField("executorRunTime").as[Int],
      c.downField("taskMetrics").downField("resultSize").as[Int],
      c.downField("taskMetrics").downField("jvmGcTime").as[Int],
      c.downField("taskMetrics").downField("peakExecutionMemory").as[Int],
      c.downField("taskMetrics").downField("inputMetrics").downField("bytesRead").as[Int],
      c.downField("taskMetrics").downField("inputMetrics").downField("recordsRead").as[Int],
      c.downField("taskMetrics").downField("outputMetrics").downField("bytesWritten").as[Int],
      c.downField("taskMetrics").downField("outputMetrics").downField("recordsWritten").as[Int],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("remoteBytesRead").as[Int],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("localBytesRead").as[Int],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("recordsRead").as[Int],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("bytesWritten").as[Int],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("writeTime").as[Int],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("recordsWritten").as[Int]
    ) mapN StageTask.apply
  }

  val jobEntityDecoder: EntityDecoder[IO, StageTasks] = jsonOf[IO, StageTasks]

}


