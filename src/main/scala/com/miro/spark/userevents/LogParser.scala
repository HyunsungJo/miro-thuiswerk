package com.miro.spark.userevents

import scopt.OParser
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{DateType}
import org.apache.spark.sql.functions._
import com.miro.spark.userevents.storage.EventStorage
import Constants._

object LogParser extends SparkJob {
  override def appName: String = AppNameLogParser

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // parse arguments
    val argsParser: ArgumentsParser = new ArgumentsParser
    val arguments: LogParserArguments = OParser.parse(argsParser.logParserArgumentsParser, args, LogParserArguments()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"invalid arguments for $appName")
    }

    // read config from arguments
    val inputPath: String = arguments.inputPath
    val outputPath: String = arguments.outputPath
    val partitionSize: Int = arguments.partitionSize
    val overwrite: Boolean = arguments.overwrite
    val storage: EventStorage = initStorage(spark, arguments.storage)

    // read event logs + add date/ time
    val rawLogsTsDf: DataFrame = storage
      .rawLogs(inputPath)
      .transform(processTimestamp())
      .repartition(partitionSize, col("event"))
      .cache()

    // write register events as parquet
    val registerEventsDf = rawLogsTsDf.transform(filterAndSelectColumns(EventValueRegistered))
    storage.writeParquetTable(registerEventsDf, EventValueRegistered, outputPath, partitionSize, overwrite)


    // write app load events as parquet
    val appLoadEventsDf = rawLogsTsDf.transform(filterAndSelectColumns(EventValueAppLoaded))
    storage.writeParquetTable(appLoadEventsDf, EventValueAppLoaded, outputPath, partitionSize, overwrite)
  }

  def processTimestamp()(ds: Dataset[_]): DataFrame = {
    ds.withColumn("time", to_timestamp(col("timestamp"), RawLogTimestampPattern))
      .withColumn("date", col("time").cast(DateType))
  }

  def filterAndSelectColumns(event: String)(df: DataFrame): DataFrame = {
    df.filter(col("event") === event)
      .transform(selectColumns(event))
  }

  private def selectColumns(event: String)(df: DataFrame): DataFrame = {
    event match {
      case "registered" => df.select("channel", ColumnsCommon: _*)
      case "app_loaded" => df.select("device_type", ColumnsCommon: _*)
      case _ => throw new Exception(s"unsupported DataSet schema")
    }
  }
}

