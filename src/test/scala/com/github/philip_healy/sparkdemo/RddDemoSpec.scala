package com.github.philip_healy.sparkdemo

import org.scalatest._

class RddDemoSpec extends FlatSpec with Matchers {
  val sampleCsvLine = """"1",1,1,"Allen, Miss. Elisabeth Walton","female",29,0,0,"24160",211.3375,"B5","S","2",NA,"St Louis, MO",1"""

  "The parseCsvLine function" should "parse CSV lines into records" in {
    val csvRecord = parseCsvLine(sampleCsvLine)
    csvRecord shouldEqual Some(List("1", "1", "1", "Allen, Miss. Elisabeth Walton", "female", "29", "0", "0", "24160",
                                    "211.3375", "B5", "S", "2", "NA", "St Louis, MO", "1"))
  }

  "The csvRecordToTitanicPassenger function" should "convert CSV records to TitanicPassenger objects" in {
    val csvRecord = List("1", "1", "1", "Allen, Miss. Elisabeth Walton", "female", "29", "0", "0", "24160",
                         "211.3375", "B5", "S", "2", "NA", "St Louis, MO", "1")
    val titanicPassenger = csvRecordToTitanicPassenger(csvRecord)
    titanicPassenger.id shouldEqual 1
    titanicPassenger.ticketClass shouldEqual Some(1)
    titanicPassenger.survived shouldEqual Some(true)
    titanicPassenger.name shouldEqual "Allen, Miss. Elisabeth Walton"
    titanicPassenger.sex shouldEqual "female"
    titanicPassenger.age shouldEqual 29
    titanicPassenger.numSpousesOrSiblingsAboard shouldEqual Some(0)
    titanicPassenger.numParentsOrChildrenAboard shouldEqual Some(0)
    titanicPassenger.ticketNumber shouldEqual "24160"
    titanicPassenger.farePaid shouldEqual "211.3375"
    titanicPassenger.cabinNumber shouldEqual "B5"
    titanicPassenger.portOfEmbarkation shouldEqual "S"
    titanicPassenger.lifeBoat shouldEqual Some("2")
    titanicPassenger.bodyId shouldEqual None
    titanicPassenger.homeDestination shouldEqual "St Louis, MO"

    //titanicPassenger.name shouldEqual ""
    //titanicPassenger.name shouldEqual ""

  }
}

