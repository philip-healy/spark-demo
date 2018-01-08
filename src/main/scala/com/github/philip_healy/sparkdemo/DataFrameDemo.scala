package com.github.philip_healy.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

// sbt "run-main com.github.philip_healy.sparkdemo.DataFrameDemo"

object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    try {
      val passengers = loadPassengersDataFrame(spark)
      passengers.cache
      passengers.printSchema
      printPassengerAgeStats(passengers)
      printSurvivalRatesByAdulthood(passengers)
      printSurvivalRatesByGender(passengers)
      printSurvivalRatesByTicketClass(passengers)
    }
    finally {
      spark.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataFrameDemo")
      .master("local")
      .getOrCreate()
  }

  def loadPassengersDataFrame(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/titanic.csv")
  }

  def printPassengerAgeStats(passengers: DataFrame): Unit = {
    println("\nAge Stats:")
    passengers.describe("age").show
    println()
  }

  def printSurvivalRatesByAdulthood(passengers: DataFrame): Unit = {
    println("\nSurvival stats by adulthood:")
    import passengers.sqlContext.implicits._
    passengers
        .withColumn("adulthood",
          when($"age" >= 18, lit("adult")).otherwise(lit("child")))
      .groupBy("adulthood")
      .agg(expr("count(*) as total"), expr("sum(survived) as survived"))
      .withColumn("survivalRate", $"survived" / $"total")
      .show()
    println()
  }

  def printSurvivalRatesByGender(passengers: DataFrame): Unit = {
    println("\nSurvival stats by gender:")
    import passengers.sqlContext.implicits._
    passengers
      .filter($"sex" === "male" || $"sex" === "female")
      .groupBy("sex")
      .agg(expr("count(*) as total"), expr("sum(survived) as survived"))
      .withColumn("survivalRate", $"survived" / $"total")
      .show()
    println()
  }

  def printSurvivalRatesByTicketClass(passengers: DataFrame): Unit = {
    println("\nSurvival stats by ticket class:")
    import passengers.sqlContext.implicits._
    passengers
      .groupBy("pclass")
      .agg(expr("count(*) as total"), expr("sum(survived) as survived"))
      .withColumn("survivalRate", $"survived" / $"total")
      .show()
    println()
  }
}
