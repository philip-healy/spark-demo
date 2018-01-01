package com.github.philip_healy.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

// sbt "run-main com.github.philip_healy.sparkdemo.DataFrameDemo"

object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val session = createSparkSession()
    try {
      val passengersDf = loadPassengersDataFrame(session)
      passengersDf.take(5).foreach(row => println(row))
    }
    finally {
      session.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataSetDemo")
      .master("local")
      .getOrCreate()
  }

  def loadPassengersDataFrame(session: SparkSession): DataFrame = {
    session
      .read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/titanic.csv")
  }
}
