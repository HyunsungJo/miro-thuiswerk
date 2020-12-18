package com.miro.spark.userevents

import com.miro.spark.userevents.storage.{EventAppLoad, EventRegister}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType}
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

    // read event logs + add date/ time
    val rawLogsTsDs = storage
      .rawLogs(inputPath)
      .transform(processTimestamp())

    storage.writeTemp(rawLogsTsDs, outputPath)


    // write register events as parquet
    val registerEvent = "registered"
    val registerLogsDf = storage.readTemp(registerEvent, outputPath)
    val registerEventsDs = toEventDs(spark, registerLogsDf, registerEvent)
    storage.writeParquetTable(registerEventsDs, registerEvent, outputPath, bucketSize, overwrite)


    // write app load events as parquet
    val appLoadEvent = "app_loaded"
    val appLoadLogsDf = storage.readTemp(appLoadEvent, outputPath)
    val appLoadEventsDs = toEventDs(spark, appLoadLogsDf, appLoadEvent)
    storage.writeParquetTable(appLoadEventsDs, appLoadEvent, outputPath, bucketSize, overwrite)

    // drop temp
    storage.dropTemp(outputPath)
  }

  def processTimestamp()(ds: Dataset[_]): DataFrame = {
    ds.withColumn("time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("date", col("time").cast(DateType))
  }

  def toEventDs(spark: SparkSession, df: DataFrame, event: String): Dataset[_] = {
    import spark.implicits._
    val dfWithEvent = df.withColumn("event", lit(event))
      .drop("browser_version", "campaign", "timestamp")
    event match {
      case "registered" => dfWithEvent
        .drop("device_type")
        .as[EventRegister]
      case "app_loaded" => dfWithEvent
        .drop("channel")
        .as[EventAppLoad]
      case _ => throw new Exception(s"unsupported DataSet schema")
    }
  }
}

