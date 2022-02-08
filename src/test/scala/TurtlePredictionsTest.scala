import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class TurtlePredictionsTest
  extends AnyFunSuite
    with BeforeAndAfter {
  var ss: SparkSession = _

  // Fatiguée
  test("TIRED : Read data and do prediction for tiny, turtle 1 at position 810284") {
    assert(TurtlePredictions.getDataAndComputePredictions("tiny", 1, 810284, 0, 0, 0, ss) == 128432258);
  }

  // Régulière
  test("REGULAR : Read data and do prediction for small, turtle 0 at position 810163") {
    assert(TurtlePredictions.getDataAndComputePredictions("small", 0, 810163, 0, 0, 0, ss) == 80206137);
  }

  // Cyclique
  test("CYCLIC : Read data and do prediction for medium, turtle 1 at position 810447") {
    assert(TurtlePredictions.getDataAndComputePredictions("medium", 1, 810447, 0, 0, 0, ss) == 111610131);
  }

  // Lunatique
  test("LUNATIC : Read data and do prediction for large, turtle 0 at position 810463") {
    assert(TurtlePredictions.getDataAndComputePredictions("large", 0, 810463, 0, 0, 0, ss) == 138498538); // TODO
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
