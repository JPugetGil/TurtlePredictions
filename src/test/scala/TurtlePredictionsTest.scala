import data.MockData
import entity.{RaceStepEntity, TurtleJourneyStepEntity}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.JsResult

import scala.collection.mutable.ListBuffer

class TurtlePredictionsTest
  extends AnyFunSuite
    with BeforeAndAfter {
  var ss: SparkSession = _

  test("Print turtle journey one by one") {
    val data: List[JsResult[RaceStepEntity]] = TurtlePredictions
      .readJsonValues(MockData.data)
    val numberOfTurtle = data.head.get.turtles.length

    val journeys = new ListBuffer[List[TurtleJourneyStepEntity]]
    for (turtleId <- 1 until numberOfTurtle) {
      journeys += TurtlePredictions.journeyOfTurtleN(turtleId, data)
    }

    journeys.foreach(journey => {
      println(journey)
      assert(journey.nonEmpty)
    })
  }

  test("Display values test") {
    assert(TurtlePredictions
      .displayValues(
        "small", 15, 8, 122, 15.2, 0.8, 42
      ))
  }

  test("Test read and display json") {
    val data: List[JsResult[RaceStepEntity]] = TurtlePredictions
      .readJsonValues(MockData.data)

    data.foreach(element => {
      assert(!element.isError)
    })
  }

  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("TurtlePredictionsTest")
      .setMaster(
        "local"
      )
    ss = SparkSession
      .builder()
      .appName("TurtlePredictionsTest")
      .config(conf)
      .getOrCreate()
  }

  after {
    ss.stop()
  }
}
