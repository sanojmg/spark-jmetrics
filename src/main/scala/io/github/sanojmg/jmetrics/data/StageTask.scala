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
                     duration: Long,
                     executorId: String,
                     host: String,
                     status: String,
                     executorRunTime: Long,
                     resultSize: Long,
                     jvmGcTime: Long,
                     peakExecutionMemory: Long,
                     bytesRead: Long,
                     recordsRead: Long,
                     bytesWritten: Long,
                     recordsWritten: Long,
                     shuffleRemoteBytesRead: Long,
                     shuffleLocalBytesRead: Long,
                     shuffleRecordsRead: Long,
                     shuffleBytesWritten: Long,
                     shuffleWriteTime: Long,
                     shuffleRecordsWritten: Long)

case class TaskAttributes( taskId: Int,
                           attempt: Int,  // dedup
                           status: String, // Eg: SUCCESS
                           duration: Long, // In seconds
                           resultSize: Long, // In bytes
                           jvmGcTime: Long, // In milliseconds
                           bytesRead: Long, // from source/persisted data
                           bytesWritten: Long,
                           shuffleBytesRead: Long,
                           shuffleBytesWritten: Long)

case class StageTaskAttr( stageId: Int,
                          attemptId: Int,
                          taskId: Int,
                          attempt: Int,  // dedup
                          status: String, // Eg: SUCCESS
                          duration: Long, // In seconds
                          resultSize: Long, // In bytes
                          jvmGcTime: Long, // In milliseconds
                          bytesRead: Long, // from source/persisted data
                          bytesWritten: Long,
                          shuffleBytesRead: Long,
                          shuffleBytesWritten: Long)

case class StageTaskAttrSt( stageId: Int,
                            attemptId: Int,
                            taskId: Int,
                            attempt: Int,  // dedup
                            status: String, // Eg: SUCCESS
                            duration: Long, // In seconds
                            resultSize: Long, // In bytes
                            jvmGcTime: Long, // In milliseconds
                            bytesRead: Long, // from source/persisted data
                            bytesWritten: Long,
                            shuffleBytesRead: Long,
                            shuffleBytesWritten: Long,
                            statusOrder: Int)

case class TaskAttributesSt( taskId: Int,
                             attempt: Int,  // dedup
                             status: String, // Eg: SUCCESS
                             duration: Long, // In seconds
                             resultSize: Long, // In bytes
                             jvmGcTime: Long, // In milliseconds
                             bytesRead: Long, // from source/persisted data
                             bytesWritten: Long,
                             shuffleBytesRead: Long,
                             shuffleBytesWritten: Long,
                             statusOrder: Int)

case class TaskDSProj( stageId: Int,
                       attemptId: Int,
                       task: StageTask,
                       status: String
                     )

case class TaskStats(avgDuration: Double,
                     maxDuration: Long,
                     avgBytesRead: Double,
                     maxBytesRead: Long,
                     avgBytesWritten: Double,
                     maxBytesWritten: Long,
                     avgShuffleBytesRead: Double,
                     maxShuffleBytesRead: Long,
                     avgShuffleBytesWritten: Double,
                     maxShuffleBytesWritten: Long
                    )

case class StageTaskStats(stageId: Int,
                          attemptId: Int,
                          avgDuration: Double,
                          maxDuration: Long,
                          avgBytesRead: Double,
                          maxBytesRead: Long,
                          avgBytesWritten: Double,
                          maxBytesWritten: Long,
                          avgShuffleBytesRead: Double,
                          maxShuffleBytesRead: Long,
                          avgShuffleBytesWritten: Double,
                          maxShuffleBytesWritten: Long
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
      c.downField("duration").as[Long],
      c.downField("executorId").as[String],
      c.downField("host").as[String],
      c.downField("status").as[String],
      c.downField("taskMetrics").downField("executorRunTime").as[Long],
      c.downField("taskMetrics").downField("resultSize").as[Long],
      c.downField("taskMetrics").downField("jvmGcTime").as[Long],
      c.downField("taskMetrics").downField("peakExecutionMemory").as[Long],
      c.downField("taskMetrics").downField("inputMetrics").downField("bytesRead").as[Long],
      c.downField("taskMetrics").downField("inputMetrics").downField("recordsRead").as[Long],
      c.downField("taskMetrics").downField("outputMetrics").downField("bytesWritten").as[Long],
      c.downField("taskMetrics").downField("outputMetrics").downField("recordsWritten").as[Long],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("remoteBytesRead").as[Long],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("localBytesRead").as[Long],
      c.downField("taskMetrics").downField("shuffleReadMetrics").downField("recordsRead").as[Long],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("bytesWritten").as[Long],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("writeTime").as[Long],
      c.downField("taskMetrics").downField("shuffleWriteMetrics").downField("recordsWritten").as[Long]
    ) mapN StageTask.apply
  }

  val jobEntityDecoder: EntityDecoder[IO, StageTasks] = jsonOf[IO, StageTasks]

}


