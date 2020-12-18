package com.miro.spark.userevents

import java.sql.{Date, Timestamp}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.miro.spark.userevents.mock.SharedSparkSession.spark
import com.miro.spark.userevents.LogParser._
import com.miro.spark.userevents.storage._

class LogParserTest extends AnyFlatSpec with should.Matchers {

  import spark.implicits._

  val timeIndex = 7
  val dateIndex = 8
  val logTsStr = "2020-12-31T22:22:22.222Z"
  val tsStr = "2020-12-31 22:22:22.222"
  val dateStr = "2020-12-31"
  val rawRegisterDs = spark.createDataset(Seq(
    EventRaw("1", logTsStr, "registered", "mobile", "2.22", "invite", "")
  ))
  val rawAppLoadDs = spark.createDataset(Seq(
    EventRaw("1", logTsStr, "app_loaded", "desktop-app", "65.0", null, null)
  ))

  val someData = Seq(
    Row(tsStr, 1L, "mobile", "0.01", "invite", "", Timestamp.valueOf(tsStr), Date.valueOf(dateStr))
  )

  val someSchema = List(
    StructField("timestamp", StringType, true),
    StructField("initiator_id", LongType, true),
    StructField("device_type", StringType, true),
    StructField("browser_version", StringType, true),
    StructField("channel", StringType, true),
    StructField("campaign", StringType, true),
    StructField("time", TimestampType, true),
    StructField("date", DateType, true)
  )

  val someDF = spark.createDataFrame(
    spark.sparkContext.parallelize(someData),
    StructType(someSchema)
  )

  "processTimestamp()" should "properly add columns time and date" in {
    val res = rawRegisterDs.transform(processTimestamp()).collect().head
    val time = res.getTimestamp(timeIndex).toString
    assert(time === tsStr)
    val date = res.getDate(dateIndex).toString
    assert(date == dateStr)
  }

  "toEventDs()" should "transform DF to Event*DS" in {
    val eventRegister = toEventDs(spark, someDF, "registered")
      .collect().head.asInstanceOf[EventRegister]
    val eventAppLoad = toEventDs(spark, someDF, "app_loaded")
      .collect().head.asInstanceOf[EventAppLoad]
    assert(eventRegister.event === "registered")
    assert(eventAppLoad.event === "app_loaded")
  }
}
