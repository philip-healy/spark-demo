package com.github.philip_healy.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

// sbt "run-main com.github.philip_healy.sparkdemo.DataFrameDemo"

object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val session = createSparkSession()
    try {
      val passengers = loadPassengersDataFrame(session)
      passengers.cache
      passengers.printSchema
      printPassengerAgeStats(passengers)
      printSurvivalRatesByGender(passengers)
    }
    finally {
      session.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataFrameDemo")
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

  def printPassengerAgeStats(passengers: DataFrame): Unit = {
    val stats = passengers.describe("age")
    println("\nAge Stats:")
    passengers.describe("age").show
    println()
  }

  def printSurvivalRatesByGender(passengers: DataFrame): Unit = {
    println("\nSurvival stats:")
    import passengers.sqlContext.implicits._
    passengers
      .filter($"sex" === "male" || $"sex" === "female")
      .groupBy("sex")
      .agg(expr("count(*) as total"), expr("sum(survived) as survived"))
      .withColumn("survivalRate", $"survived" / $"total")
      .show()
    //passengers.createOrReplaceTempView("passengers")
    //passengers.sqlContext.sql("select * from passengers group by sex").show
  }
}
