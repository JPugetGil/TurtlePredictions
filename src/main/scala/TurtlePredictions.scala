import entity.behavior.BehaviorFormatter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.DataPredictionUtils


object TurtlePredictions {
  val compte = "p1608911"

  def getDataAndComputePredictions(
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
                                  ): Int = {
    val row = spark.read.csv("%s-analysis.csv".format(course))
      .filter(element => element(0).asInstanceOf[String].toInt == turtleId)
      .head()

    val turtleTypeEntity = BehaviorFormatter.toEntity(row)
    DataPredictionUtils.doTurtlePrediction(turtleTypeEntity, top, position1, position2, position3, temperature, qualite, deltaTop)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("TurtlePredictions-" + compte)
      val spark = SparkSession
        .builder()
        .appName("TurtlePredictions")
        .config(conf)
        .getOrCreate()

      if (args.length == 9) {
        val course: String = args(0)
        val turtleId: Int = args(1).toInt
        val top: Int = args(2).toInt
        val position1: Int = args(3).toInt
        val position2: Int = args(4).toInt
        val position3: Int = args(5).toInt
        val temperature: Double = args(6).toDouble
        val qualite: Double = args(7).toDouble
        val deltaTop: Int = args(8).toInt

        getDataAndComputePredictions(course, turtleId, top, position1, position2, position3, temperature, qualite, deltaTop, spark)

      } else {
        // Exemple d'une tortue régulière avec pas de 2
        println(
          "Usage: spark-submit --class TurtlePredictions /home/" + compte + "/TurtlePredictions-assembly-1.0.jar " +
            "small" +
            "6" +
            "20000" +
            "42" +
            "44" +
            "46" +
            "15.823" +
            "0.66" +
            "15"
        )
      }
    }
  }
}
