import entity.behavior.BehaviorFormatter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.DataPredictionUtils


object TurtlePredictions {
  val compte = "p1608911"

  def getDataAndComputePredictions(raceType: String, turtleId: Int, position: Int, temp: Double, qual: Double, deltatop: Int, ss: SparkSession): Int = {
    val row = ss.read.csv("%s-analysis.csv".format(raceType))
      .filter(element => element(0).asInstanceOf[String].toInt == turtleId)
      .head()

    val turtleTypeEntity = BehaviorFormatter.toEntity(row)
    DataPredictionUtils.doTurtlePrediction(turtleTypeEntity, position, temp, qual, deltatop)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("TurtleAnalysis-" + compte)
      val spark = SparkSession
        .builder()
        .appName("TurtleAnalysis")
        .config(conf)
        .getOrCreate()

      if (args.length == 7) {
        val course: String = args(0)
        val turtleId: Int = args(1).toInt
        val top: Int = args(2).toInt
        val position: Int = args(3).toInt
        val temperature: Double = args(4).toDouble
        val qualite: Double = args(5).toDouble
        val deltaTop: Int = args(6).toInt

      } else {
        println(
          "Usage: spark-submit --class TurtlePredictions /home/" + compte + "/TurtlePredictions-assembly-1.0.jar " +
            "small" +
            "6" +
            "20000" +
            "42" +
            "15.823" +
            "0.66" +
            "15"
        )
      }
    }
  }
}
