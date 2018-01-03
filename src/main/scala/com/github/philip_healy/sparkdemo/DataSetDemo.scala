package com.github.philip_healy.sparkdemo


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

// sbt "run-main com.github.philip_healy.sparkdemo.DataSetDemo"

case class TitanicPassenger2()

object DataSetDemo {
  /*
  def main(args: Array[String]): Unit = {
    val session = createSparkSession()
    try {
      val passengers = loadPassengerDataset(session)
      passengers.cache
      printPassengerAgeStats(passengers)
      println("\nTADA!\n")
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

  def loadPassengerDataset(session: SparkSession): Dataset[TitanicPassenger2] = {
    val schema = ScalaReflection.schemaFor[TitanicPassenger2].dataType.asInstanceOf[StructType]
    session.read
      .option("header", "true")
      .schema(schema)
      .csv("./src/main/resources/titanic.csv")
      .as[TitanicPassenger2]
  }

  def printPassengerAgeStats(passengers: Dataset[TitanicPassenger2]) = {

  }
  */
}
