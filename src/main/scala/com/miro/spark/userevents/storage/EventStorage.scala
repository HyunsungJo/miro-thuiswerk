package com.miro.spark.userevents.storage

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.miro.spark.userevents.Constants._

/**
 * Storage defines all input and output logic - how and where tables and files
 * should be read and saved.
 */
trait EventStorage {
  def rawLogs(path: String): Dataset[EventRaw]

  def writeParquetTable(df: DataFrame, event: String, outputPath: String, bucketSize: Int, overwrite: Boolean): Unit

  def readParquetTable(event: String, outputPath: String): Dataset[_]
}

class LocalEventStorage(spark: SparkSession) extends EventStorage {

  import spark.implicits._

  private def readJSON[T: Encoder](location: String) = spark.read
    .json(location)
    .as[T]

  override def rawLogs(inputPath: String): Dataset[EventRaw] = readJSON[EventRaw](inputPath)

  override def writeParquetTable(
    df: DataFrame,
    event: String,
    outputPath: String,
    partitionSize: Int = 200,
    overwrite: Boolean = false): Unit = {

    val saveMode: SaveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists

    val ds: Dataset[_] = event match {
      case EventValueRegistered => df.as[EventRegister]
      case EventValueAppLoaded => df.as[EventAppLoad]
      case _ => throw new Exception(s"Unsupported event type")
    }

    ds.repartition(partitionSize, col("date"))
      .write
      .mode(saveMode)
      .partitionBy("date")
      .format("parquet")
      .option("path", s"$outputPath/$event")
      .saveAsTable(event)
  }

  override def readParquetTable(event: String, path: String): Dataset[_] = {
    event match {
      case EventValueRegistered => spark.read
        .parquet(s"$path/$event")
        .as[EventRegister]
      case EventValueAppLoaded => spark.read
        .parquet(s"$path/$event")
        .as[EventAppLoad]
      case _ => throw new Exception(s"Unsupported event type")
    }
  }
}

// TODO: implement some other storage for production - S3, HDFS, etc.
class RemoteEventStorage(spark: SparkSession) extends EventStorage {
  override def rawLogs(path: String): Dataset[EventRaw] = ???

  override def writeParquetTable(
    df: DataFrame,
    event: String,
    outputPath: String,
    bucketSize: Int,
    overwrite: Boolean): Unit = ???

  override def readParquetTable(event: String, outputPath: String): Dataset[_] = ???
}

case class EventRaw(
  initiator_id: String,
  timestamp: String,
  event: String,
  device_type: String,
  browser_version: String,
  channel: String,
  campaign: String
)

case class EventAppLoad(
  event: String = EventValueAppLoaded,
  time: java.sql.Timestamp,
  initiator_id: Long,
  device_type: String = "n/a",
  date: java.sql.Date
)

case class EventRegister(
  event: String = EventValueRegistered,
  time: java.sql.Timestamp,
  initiator_id: Long,
  channel: String = "n/a",
  date: java.sql.Date
)

