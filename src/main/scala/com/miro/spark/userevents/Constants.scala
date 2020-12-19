package com.miro.spark.userevents

object Constants {
  val ConfDefaultPartitionSize = 200
  val AppNameLogParser = "LogParser"
  val AppNameStatsGenerator = "StatsGenerator"
  val EventValueRegistered = "registered"
  val EventValueAppLoaded = "app_loaded"
  val ColumnsCommon = List("event", "time", "initiator_id", "date")
  val RawLogTimestampPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val ColumnJoinKey = "join_key"
}
