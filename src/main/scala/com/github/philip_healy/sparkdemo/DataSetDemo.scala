package com.github.philip_healy.sparkdemo


import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

// sbt "run-main com.github.philip_healy.sparkdemo.DataSetDemo"

case class Passenger(
  val id: Int,
  val ticketClass: Option[Int],
  val survived: Option[Boolean],
  val name: String,
  val sex: String,
  val age: Double,
  val numSpousesOrSiblingsAboard: Option[Int],
  val numParentsOrChildrenAboard: Option[Int],
  val ticketNumber: String,
  val farePaid: String,
  val cabinNumber: String,
  val portOfEmbarkation: String,
  val lifeBoat: String,
  val bodyId: String,
  val homeDestination: String)

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    try {
      val passengers = loadPassengerDataset(spark)
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
      .appName("DataSetDemo")
      .master("local")
      .getOrCreate()
  }

  def loadPassengerDataset(spark: SparkSession): Dataset[Passenger] = {
    import spark.implicits._
    spark
      .read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/titanic.csv")
      .withColumnRenamed("_c0", "id")
      .withColumn("id", 'id.cast(IntegerType))
      .withColumnRenamed("pclass", "ticketClass")
      .withColumn("ticketClass", 'ticketClass.cast(IntegerType))
      .withColumn("age", 'age.cast(DoubleType))
      .withColumnRenamed("sibsp", "numSpousesOrSiblingsAboard")
      .withColumn("numSpousesOrSiblingsAboard",  'numSpousesOrSiblingsAboard.cast(IntegerType))
      .withColumnRenamed("parch", "numParentsOrChildrenAboard")
      .withColumn("numParentsOrChildrenAboard",  'numParentsOrChildrenAboard.cast(IntegerType))
      .withColumnRenamed("ticket", "ticketNumber")
      .withColumnRenamed("fare", "farePaid")
      .withColumnRenamed("cabin", "cabinNumber")
      .withColumnRenamed("embarked", "portOfEmbarkation")
      .withColumnRenamed("boat", "lifeBoat")
      .withColumnRenamed("body", "bodyId")
      .withColumnRenamed("home.dest", "homeDestination")
      .drop("had_cabin_number")
      .as[Passenger]
  }

  def printPassengerAgeStats(passengers: Dataset[Passenger]): Unit = {
    println("\nAge Stats:")
    passengers.describe("age").show
    println()
  }

  // See https://github.com/rohgar/scala-spark-4/wiki/Datasets
  case class SurvivalCount (numTotal: Int, numSurvived: Int)
  case class SurvivalStats (numTotal: Int, numSurvived: Int, survivalRate: Double)
  class SurvivalAggregator extends Aggregator[Passenger, SurvivalCount, SurvivalStats] {

    def zero: SurvivalCount = SurvivalCount(0, 0)

    def reduce(count: SurvivalCount, passenger: Passenger): SurvivalCount =
      SurvivalCount(count.numTotal + 1,
                    count.numSurvived + (if (passenger.survived == Some(true)) 1 else 0))

    def merge(count1: SurvivalCount, count2: SurvivalCount): SurvivalCount =
      SurvivalCount(count1.numTotal + count2.numTotal,
        count1.numSurvived + count2.numSurvived)

    def finish(count: SurvivalCount): SurvivalStats =
      SurvivalStats(count.numTotal, count.numSurvived,
        count.numSurvived.toDouble / count.numTotal.toDouble)

    override def bufferEncoder: Encoder[SurvivalCount] = Encoders.product[SurvivalCount]

    override def outputEncoder: Encoder[SurvivalStats] = Encoders.product[SurvivalStats]
  }


  def printSurvivalRatesByAdulthood(passengers: Dataset[Passenger]): Unit = {
    println("\nSurvival stats by adulthood:")

    import passengers.sparkSession.implicits._

    passengers
      .groupByKey(p => if (p.age >= 18) "adult" else "child")
      .agg((new SurvivalAggregator).toColumn)
      .show(false)

    println()
  }


  def printSurvivalRatesByGender(passengers: Dataset[Passenger]): Unit = {
    println("\nSurvival stats by gender:")

    import passengers.sparkSession.implicits._

    passengers
      .filter(p => p.sex == "male" || p.sex == "female")
      .groupByKey(_.sex)
      .agg((new SurvivalAggregator).toColumn)
      .show(false)

    println()
  }

  def printSurvivalRatesByTicketClass(passengers: Dataset[Passenger]): Unit = {
    println("\nSurvival stats by gender:")

    import passengers.sparkSession.implicits._

    passengers
      .groupByKey(_.ticketClass)
      .agg((new SurvivalAggregator).toColumn)
      .show(false)

    println()
  }
}
