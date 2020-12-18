package com.miro.spark.userevents.storage

import java.io.File

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SaveMode, SparkSession}

import scala.reflect.io.Directory

/**
 * Storage defines all input and output logic - how and where tables and files
 * should be read and saved.
 */
trait EventStorage {
  def rawLogs(path: String): Dataset[EventRaw]

  def writeParquetTable(ds: Dataset[_], event: String, outputPath: String, bucketSize: Int, overwrite: Boolean): Unit

  def readParquetTable(event: String, outputPath: String): Dataset[_]

  def writeTemp(ds: Dataset[_], outputPath: String): Unit

  def readTemp(event: String, outputPath: String): DataFrame

  def dropTemp(outputPath: String): Unit
}

class LocalEventStorage(spark: SparkSession) extends EventStorage {

  import spark.implicits._

  private def readJSON[T: Encoder](location: String) = spark.read
    .json(location)
    .as[T]

  override def rawLogs(inputPath: String): Dataset[EventRaw] = readJSON[EventRaw](inputPath)

  override def writeParquetTable(ds: Dataset[_],
    event: String,
    outputPath: String,
    bucketSize: Int = 10,
    overwrite: Boolean = false): Unit = {

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists

    ds.write
      .mode(saveMode)
      .bucketBy(bucketSize, "date")
      .sortBy("date")
      .format("parquet")
      .option("path", s"$outputPath/$event")
      .saveAsTable(event)
  }

  override def readParquetTable(event: String, path: String): Dataset[_] = {
    event match {
      case "registered" => spark.read
        .parquet(s"$path/$event")
        .as[EventRegister]
      case "app_loaded" => spark.read
        .parquet(s"$path/$event")
        .as[EventAppLoad]
      case _ => throw new Exception(s"Unsupported event type")
    }
  }

  override def writeTemp(ds: Dataset[_], path: String): Unit = ds.write
    .mode(SaveMode.Overwrite) // temp data always overwrites
    .partitionBy("event")
    .format("parquet")
    .save(s"$path/temp")

  override def readTemp(event: String, path: String): DataFrame = spark.read
    .parquet(s"$path/temp/event=$event")

  override def dropTemp(path: String): Unit = {
    val targetDir = new Directory(new File(s"$path/temp"))
    targetDir.deleteRecursively()
  }
}

// TODO: implement some other storage for production - S3, HDFS, etc.
class RemoteEventStorage(spark: SparkSession) extends EventStorage {
  override def rawLogs(path: String): Dataset[EventRaw] = ???

  override def writeParquetTable(ds: Dataset[_], event: String, outputPath: String, bucketSize: Int, overwrite: Boolean): Unit = ???

  override def readParquetTable(event: String, outputPath: String): Dataset[_] = ???

  override def writeTemp(ds: Dataset[_], outputPath: String): Unit = ???

  override def readTemp(event: String, outputPath: String): DataFrame = ???

  override def dropTemp(outputPath: String): Unit = ???
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
  event: String = "app_loaded",
  time: java.sql.Timestamp,
  initiator_id: Long,
  device_type: String,
  date: java.sql.Date
)

case class EventRegister(
  event: String = "registered",
  time: java.sql.Timestamp,
  initiator_id: Long,
  channel: String,
  date: java.sql.Date
)

