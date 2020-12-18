package com.miro.spark.userevents

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec._
import org.scalatest.matchers._
import com.miro.spark.userevents.mock.SharedSparkSession.spark
import com.miro.spark.userevents.storage.{LocalEventStorage, RemoteEventStorage}

class SparkJobTestTest extends AnyFlatSpec with should.Matchers {

  val mockAppName = "mock-spark-jab"
  val message = "this is a mock job"
  var result = ""
  object MockSparkJob extends SparkJob {
    override def appName: String = mockAppName
    override def run(spark: SparkSession, args: Array[String]): Unit = {
      result = message
    }
  }

  val mockSparkJob = MockSparkJob

  "SparkJob" should "initStorage" in {
    val localStore = mockSparkJob.initStorage(spark, "local")
    assert(localStore.isInstanceOf[LocalEventStorage])
    val remoteStore = mockSparkJob.initStorage(spark, "remote")
    assert(remoteStore.isInstanceOf[RemoteEventStorage])
  }
}
