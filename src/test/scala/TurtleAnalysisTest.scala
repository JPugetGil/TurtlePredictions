import data.MockData
import entity.{RaceStepEntity, TurtleJourneyStepEntity}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.JsResult

import scala.collection.mutable.ListBuffer

class TurtleAnalysisTest
  extends AnyFunSuite
    with BeforeAndAfter {
  var ss: SparkSession = _

  test("Print turtle journey one by one") {
    val data: List[JsResult[RaceStepEntity]] = TurtleAnalysis
      .readJsonValues(MockData.data)
    val numberOfTurtle = data.head.get.turtles.length

    val journeys = new ListBuffer[List[TurtleJourneyStepEntity]]
    for (turtleId <- 1 until numberOfTurtle) {
      journeys += TurtleAnalysis.journeyOfTurtleN(turtleId, data)
    }

    journeys.foreach(journey => {
      println(journey)
      assert(journey.nonEmpty)
    })
  }

  test("Display values test") {
    assert(TurtleAnalysis
      .displayValues(
        "small", 15, 8, 122, 15.2, 0.8, 42
      ))
  }

  test("Test read and display json") {
    val data: List[JsResult[RaceStepEntity]] = TurtleAnalysis
      .readJsonValues(MockData.data)

    data.foreach(element => {
      assert(!element.isError)
    })
  }

  test("Read data and do LR for tiny") {
    assert(TurtleAnalysis.getDataAndComputeLR("python", "tiny", ss));
  }

  test("Read data and do LR for small") {
    assert(TurtleAnalysis.getDataAndComputeLR("python", "small", ss));
  }

  test("Read data and do LR for medium") {
    assert(TurtleAnalysis.getDataAndComputeLR("python", "medium", ss));
  }

  test("Read data and do LR for large") {
    assert(TurtleAnalysis.getDataAndComputeLR("python", "large", ss));
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
