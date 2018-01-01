package com.github.philip_healy.sparkdemo

import org.apache.spark.sql.SparkSession

// sbt "run-main com.github.philip_healy.sparkdemo.DataSetDemo"

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val sc = createSparkSession()
    try {
      println("\nTADA!\n")
    }
    finally {
      sc.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataSetDemo")
      .master("local")
      .getOrCreate()
  }
}
