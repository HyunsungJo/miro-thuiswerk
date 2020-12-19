package com.miro.spark.userevents

import java.sql.{Date, Timestamp}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import com.miro.spark.userevents.StatsGenerator._
import com.miro.spark.userevents.mock.SharedSparkSession.spark
import com.miro.spark.userevents.storage.EventRegister

class StatsGeneratorTest extends AnyFlatSpec with should.Matchers {
  import spark.implicits._

  val lastIndex = 5
  val tsStr = "2020-12-31 22:22:22"
  val dateStr = "2020-12-31"
  val mockDs = spark.createDataset(Seq(
    EventRegister("registered", Timestamp.valueOf(tsStr), 1L, "invite", Date.valueOf(dateStr)),
    EventRegister("registered", Timestamp.valueOf(tsStr), 2L, "invite", Date.valueOf(dateStr)),
    EventRegister("registered", Timestamp.valueOf(tsStr), 1L, "invite", Date.valueOf(dateStr)),
  ))

  "roundAt()" should "round a Double at given precision" in {
    val d: Double = 3.141592
    val rounded = roundAt(d, 2)
    assert(rounded === BigDecimal(3.14))
  }

  "getThisYearKey()" should "get THIS YEAR" in {
    val year = mockDs.transform(addJoinKey(getThisYearKey))
      .collect().head.getInt(lastIndex)
    assert(year === 2020)
  }

  "getNextYearKey()" should "get NEXT YEAR" in {
    val year = mockDs.transform(addJoinKey(getNextYearKey))
      .collect().head.getInt(lastIndex)
    assert(year === 2021)
  }

  "getThisMonthKey()" should "get THIS MONTH" in {
    val month = mockDs.transform(addJoinKey(getThisMonthKey))
      .collect().head.getString(lastIndex)
    assert(month === "2020-12")
  }

  "getNextMonthKey()" should "get NEXT MONTH" in {
    val month = mockDs.transform(addJoinKey(getNextMonthKey))
      .collect().head.getString(lastIndex)
    assert(month === "2021-1")
  }

  "getThisWeekKey()" should "get THIS WEEK number" in {
    val week = mockDs.transform(addJoinKey(getThisWeekKey))
      .collect().head.getString(lastIndex)
    assert(week === "2020-53")
  }

  "getNextWeekKey()" should "get NEXT WEEK number" in {
    val week = mockDs.transform(addJoinKey(getNextWeekKey))
      .collect().head.getString(lastIndex)
    assert(week === "2021-1")
  }

  "aggCountDistinctUsers()" should "return dist user count" in {
    val userCount = aggCountDistinctUsers(mockDs.toDF())
    assert(userCount === 2.0)
  }

  "aggCountDistinctUsers()" should "return 0.0 for empty DF" in {
    val emptyDf = spark.emptyDataset[EventRegister].toDF()
    val userCount = aggCountDistinctUsers(emptyDf)
    assert(userCount === 0.0)
  }
}
