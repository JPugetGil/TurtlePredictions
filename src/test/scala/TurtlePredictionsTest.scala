import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class TurtlePredictionsTest
  extends AnyFunSuite
    with BeforeAndAfter {
  var ss: SparkSession = _

  /*
                                    course: String,
                                    turtleId: Int,
                                    top: Int,
                                    position1: Int,
                                    position2: Int,
                                    position3: Int,
                                    temperature: Double,
                                    qualite: Double,
                                    deltaTop: Int,
                                    spark: SparkSession
  */

  // FIXME : Tester
  // Fatiguée
  test("TIRED : Read data and do prediction for tiny, turtle 1 at position 810284") {
    assert(TurtlePredictions.getDataAndComputePredictions(
      "tiny",
      1,
      893127,
      141559689,
      141559724,
      141559764,
      40,
      0.3100529516794682,
      64,
      ss
    ) == 141571778);
  }


  // Régulière
  test("REGULAR : Read data and do prediction for small, turtle 0 at position 810163") {
    assert(TurtlePredictions.getDataAndComputePredictions(
      "small",
      0,
      810163,
      80206137,
      80206236,
      80206335,
      40,
      0.3100529516794682,
      30,
      ss
    ) == 80209107)
  }

  // Cyclique
  test("CYCLIC : Read data and do prediction for medium, turtle 1 at position 810447") {
    assert(TurtlePredictions.getDataAndComputePredictions(
      "medium",
      1,
      893905,
      123103621,
      123103695,
      123103764,
      40,
      0.3100529516794682,
      90,
      ss
    ) == 123115943);
  }

  /*
  // FIXME : Tester
  // Lunatique
  test("LUNATIC : Read data and do prediction for large, turtle 0 at position 810463") {
    assert(TurtlePredictions.getDataAndComputePredictions("large", 0, 810463, 0, 0, 0, ss) == 138498538);
  }*/

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
