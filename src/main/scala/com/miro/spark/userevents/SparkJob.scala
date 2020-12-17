package com.miro.spark.userevents

import com.miro.spark.userevents.storage.{EventStorage, LocalEventStorage, RemoteEventStorage}
import org.apache.spark.sql.SparkSession

trait SparkJob {

  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark event-processor")
        .getOrCreate()
    }

    run(spark, args)
  }

  def run(spark: SparkSession, args: Array[String]): Unit

  def appName: String

  def initStorage(spark: SparkSession, storage: String): EventStorage = if (storage == "local") {
    new LocalEventStorage(spark)
  } else {
    // TODO: implement remote storage
    new RemoteEventStorage(spark)
  }
}
