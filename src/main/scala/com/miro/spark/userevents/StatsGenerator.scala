package com.miro.spark.userevents

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OParser

object StatsGenerator extends SparkJob {

  override def appName: String = "stats-generator"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // parse arguments
    val argsParser = new ArgumentsParser
    val arguments = OParser.parse(argsParser.statsGeneratorArgumentsParser, args, StatsGeneratorArguments()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"invalid arguments for $appName")
    }

    // read config from arguments
    val tablePath = arguments.tablePath
    val period = arguments.period
    val storage = initStorage(spark, arguments.storage)


    // functions to get keys of current + next period
    val (thisPeriodFunc, nextPeriodFunc) = getPeriodFunctions(period)

    // process register events: add join col and rename duplicate cols
    val registerEventsDs = storage.readParquetTable("registered", tablePath)
    val registerEventsDf = addPeriodKey(registerEventsDs, nextPeriodFunc)
      .withColumnRenamed("initiator_id", "reg_initiator_id")
      .withColumnRenamed("period_key", "next_period_key")

    // process app load events: add join col
    val appLoadEventsDs = storage.readParquetTable("app_loaded", tablePath)
    val appLoadEventsDf = addPeriodKey(appLoadEventsDs, thisPeriodFunc)

    // join dataframes on period_key and initiator_id to get target users
    // TODO: consider broadcast
    val targetUsersDf = appLoadEventsDf
      .join(registerEventsDf, appLoadEventsDf("period_key") === registerEventsDf("next_period_key") &&
        appLoadEventsDf("initiator_id") === registerEventsDf("reg_initiator_id"), "inner")

    // union dataframes to get total users
    val totalUsersDf = appLoadEventsDf
      .select("initiator_id")
      .union(registerEventsDf
        .select("reg_initiator_id"))

    // distinct user count
    val targetUserCount: Double = aggCountDistinctUsers(targetUsersDf)
    val totalUserCount: Double = aggCountDistinctUsers(totalUsersDf)

    // print result
    val formulaStr = s"target: $targetUserCount / total: $totalUserCount"
    println("==========")
    println("")
    if (totalUserCount == 0) {
      println(formulaStr)
    } else {
      println(s"$formulaStr = ${(targetUserCount.toDouble / totalUserCount.toDouble) * 100}%")
    }
    println("")
    println("==========")
  }

  def aggCountDistinctUsers(df: DataFrame): Double = {
    // TODO: use `approx_count_distinct()` for speed over accuracy
    val aggDf = df.agg(countDistinct("initiator_id")).cache()

    if (aggDf.head(1).isEmpty) {
      0.0
    } else {
      aggDf.collect()(0)(0)
        .asInstanceOf[Long]
        .toDouble
    }
  }

  def getPeriodFunctions(period: String):(Column => Column, Column => Column) = {
    val m: Map[String, (Column => Column, Column => Column)] = Map(
      "week" -> (getThisWeekKey, getNextWeekKey),
      "month" -> (getThisMonthKey, getNextMonthKey),
      "year" -> (getThisYearKey, getNextYearKey),
    )
    m("week")
  }

  def addPeriodKey(ds: Dataset[_], f: Column => Column): DataFrame = {
    ds.withColumn("period_key", f(col("time")))
  }

  def getThisWeekKey(column: Column): Column = {
    concat(year(column), lit("-"), weekofyear(column))
  }

  def getNextWeekKey(column: Column): Column = {
    val DaysInWeek = 7
    val newDay = date_add(column, DaysInWeek)
    concat(year(newDay), lit("-"), weekofyear(newDay))
  }

  def getThisMonthKey(column: Column): Column = {
    concat(year(column), lit("-"), month(column))
  }

  def getNextMonthKey(column: Column): Column = {
    val newDay = add_months(column, 1)
    concat(year(newDay), lit("-"), month(newDay))
  }

  def getThisYearKey(column: Column): Column = year(column)

  def getNextYearKey(column: Column): Column = {
    val DaysInYear = 365
    val newDay = date_add(column, DaysInYear)
    year(newDay)
  }
}

