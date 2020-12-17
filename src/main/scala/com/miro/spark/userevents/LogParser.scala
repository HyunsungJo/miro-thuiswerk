package com.miro.spark.userevents

import com.miro.spark.userevents.storage.{EventRegister, EventAppLoad}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import scopt.OParser

object LogParser extends SparkJob {
  override def appName: String = "log-parser"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // parse arguments
    val argsParser = new ArgumentsParser
    val arguments = OParser.parse(argsParser.logParserArgumentsParser, args, LogParserArguments()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"invalid arguments for $appName")
    }

    // read config from arguments
    val inputPath = arguments.inputPath
    val outputPath = arguments.outputPath
    val bucketSize = arguments.bucketSize
    val overwrite = arguments.overwrite
    val storage = initStorage(spark, arguments.storage)

    // read event logs
    val rawLogsDs = storage.rawLogs(inputPath)

    // process ts
    val tsProcessedDs = processTimestamp(rawLogsDs)
    storage.writeTemp(tsProcessedDs, outputPath)


    // write register events as parquet
    val registerEvent = "registered"
    val registerLogsDf = storage.readTemp(registerEvent, outputPath)
    val registerEventsDs = asEvent(spark, registerLogsDf, registerEvent)
    storage.writeParquetTable(registerEventsDs, registerEvent, outputPath, bucketSize, overwrite)


    // write app load events as parquet
    val appLoadEvent = "app_loaded"
    val appLoadLogsDf = storage.readTemp(appLoadEvent, outputPath)
    val appLoadEventsDs = asEvent(spark, appLoadLogsDf, appLoadEvent)
    storage.writeParquetTable(appLoadEventsDs, appLoadEvent, outputPath, bucketSize, overwrite)

    // drop temp
    storage.dropTemp(outputPath)
  }

  def processTimestamp(ds: Dataset[_]): DataFrame = {
    ds.withColumn("time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("date", col("time").cast(DateType))
  }

  def asEvent(spark: SparkSession, df: DataFrame, event: String): Dataset[_] = {
    import spark.implicits._
    val dfWithEvent = df.withColumn("event", lit(event))
    event match {
      case "registered" => dfWithEvent.as[EventRegister]
      case "app_loaded" => dfWithEvent.as[EventAppLoad]
      case _ => throw new Exception(s"unsupported DataSet schema")
    }
  }
}

