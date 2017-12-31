package com.github.philip_healy.sparkdemo

import com.github.tototoshi.csv
import com.github.tototoshi.csv.CSVParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
//import org.apache.spark.mllib.stat.Statistics
//import org.apache.spark.SparkContext._


// sbt "run-main com.github.philip_healy.sparkdemo.RddDemo"

case class TitanicPassenger(
  val id: Int,
  val ticketClass: Option[Int],
  val survived: Option[Boolean],
  val name: String,
  val sex: String,
  val age: Int,
  val numSpousesOrSiblingsAboard: Option[Int],
  val numParentsOrChildrenAboard: Option[Int],
  val ticketNumber: String,
  val farePaid: String,
  val cabinNumber: String,
  val portOfEmbarkation: String,
  val lifeBoat: Option[String],
  val bodyId: Option[String],
  val homeDestination: String)


object parseCsvLine {
  val csvParser = new CSVParser(csv.defaultCSVFormat)
  def apply(csvLine: String): Option[List[String]] = {
    csvParser.parseLine(csvLine)
  }
}

object csvRecordToTitanicPassenger {
  def apply(csvRecord: List[String]): TitanicPassenger = {
    TitanicPassenger(
      id = csvRecord(0).toInt,
      ticketClass = csvRecord(1) match {case "NA" => None; case _ => Some(csvRecord(1).toInt)},
      survived = csvRecord(2) match {case "NA" => None; case "0" => Some(false); case "1" => Some(true)},
      name = csvRecord(3),
      sex = csvRecord(4),
      age = csvRecord(5).toDouble.toInt,
      numSpousesOrSiblingsAboard = csvRecord(6) match {case "NA" => None; case _ => Some(csvRecord(6).toInt)},
      numParentsOrChildrenAboard = csvRecord(7) match {case "NA" => None; case _ => Some(csvRecord(7).toInt)},
      ticketNumber = csvRecord(8),
      farePaid = csvRecord(9),
      cabinNumber = csvRecord(10),
      portOfEmbarkation = csvRecord(11),
      lifeBoat = csvRecord(12) match {case "NA" => None; case _ => Some(csvRecord(12))},
      bodyId = csvRecord(13) match {case "NA" => None; case _ => Some(csvRecord(13))},
      homeDestination = csvRecord(14)
    )
  }
}

object RddDemo {
  def main(args: Array[String]): Unit = {
    val sc = createSparkContext()
    try {
      val passengers = readPassengerData(sc)
      passengers.cache
      printPassengerAgeStats(passengers)
    }
    finally {
      sc.stop()
    }
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RddDemo")
    new SparkContext(conf)
  }

  def readPassengerData(sc: SparkContext): RDD[TitanicPassenger] =  {
    val inputFile = sc.textFile("./src/main/resources/titanic.csv")
    val header = inputFile.first()
    val dataCsvLines = inputFile.filter(row => row != header)
    val csvRecords = dataCsvLines.flatMap(line => parseCsvLine(line))
    val passengers = csvRecords.map(csvRecord => csvRecordToTitanicPassenger(csvRecord))
    passengers
  }

  def printPassengerAgeStats(passengers: RDD[TitanicPassenger]): Unit = {
    val ages = passengers.map(_.age.toDouble)
    val stats = ages.stats
    println("\n")
    println(s"Summary statistics for passenger ages: $stats")
    println("\n")
  }
}