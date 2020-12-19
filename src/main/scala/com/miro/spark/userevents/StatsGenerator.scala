package com.miro.spark.userevents

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OParser
import com.miro.spark.userevents.storage.EventStorage
import Constants._

object StatsGenerator extends SparkJob {

  override def appName: String = Constants.AppNameStatsGenerator

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    // parse arguments
    val argsParser: ArgumentsParser = new ArgumentsParser
    val arguments: StatsGeneratorArguments = OParser.parse(argsParser.statsGeneratorArgumentsParser, args, StatsGeneratorArguments()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException(s"invalid arguments for $appName")
    }

    // read config from arguments
    val tablePath: String = arguments.tablePath
    val period: String = arguments.period
    val storage: EventStorage = initStorage(spark, arguments.storage)

    // functions to get keys of current + next period
    val (thisPeriodFunc, nextPeriodFunc) = getPeriodFunctions(period)

    // process register events: add join col and rename duplicate cols
    val registerEventsDf: DataFrame = storage
      .readParquetTable(EventValueRegistered, tablePath)
      .withColumnRenamed("initiator_id", "reg_initiator_id")
      .transform(addJoinKey(nextPeriodFunc))
      .repartition(ConfDefaultPartitionSize, col(ColumnJoinKey))

    // process app load events: add join col
    val appLoadEventsDf: DataFrame = storage
      .readParquetTable(EventValueAppLoaded, tablePath)
      .transform(addJoinKey(thisPeriodFunc))
      .repartition(ConfDefaultPartitionSize, col(ColumnJoinKey))

    // join dataframes on period_key and initiator_id to get target users
    // TODO: consider broadcast
    val targetUsersDf: DataFrame = appLoadEventsDf
      .join(registerEventsDf, appLoadEventsDf(ColumnJoinKey) === registerEventsDf(ColumnJoinKey) &&
        appLoadEventsDf("initiator_id") === registerEventsDf("reg_initiator_id"), "inner")

    // union dataframes to get total users
    val totalUsersDf: DataFrame = appLoadEventsDf
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
      println(s"$formulaStr = metric: ${roundAt((targetUserCount.toDouble / totalUserCount.toDouble)*100, 2)} %")
    }
    println("")
    println("==========")
  }

  def aggCountDistinctUsers(df: DataFrame): Double = {
    // TODO: use `approx_count_distinct()` for speed over accuracy
    val aggDf: DataFrame = df.agg(countDistinct("initiator_id"))

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
      "year" -> (getThisYearKey, getNextYearKey)
    )
    m("week")
  }

  def addJoinKey(f: Column => Column)(ds: Dataset[_]): DataFrame = {
    ds.withColumn(ColumnJoinKey, f(col("time")))
  }

  def getThisWeekKey(column: Column): Column = {
    concat(year(column), lit("-"), weekofyear(column))
  }

  def getNextWeekKey(column: Column): Column = {
    val DaysInWeek = 7
    val newDay: Column = date_add(column, DaysInWeek)
    concat(year(newDay), lit("-"), weekofyear(newDay))
  }

  def getThisMonthKey(column: Column): Column = {
    concat(year(column), lit("-"), month(column))
  }

  def getNextMonthKey(column: Column): Column = {
    val newDay: Column = add_months(column, 1)
    concat(year(newDay), lit("-"), month(newDay))
  }

  def getThisYearKey(column: Column): Column = year(column)

  def getNextYearKey(column: Column): Column = {
    val MonthsInYear = 12
    val newDay: Column = add_months(column, MonthsInYear)
    year(newDay)
  }

  def roundAt(n: Double, p: Int): BigDecimal = BigDecimal(n.toFloat).setScale(p, BigDecimal.RoundingMode.HALF_UP)

}

