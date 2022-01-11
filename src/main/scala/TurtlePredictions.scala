import org.apache.spark.{SparkConf, SparkContext}

import java.lang.Math.{max, min}

object TurtlePredictions {
  val compte = "p1608911"

  def displayValues(course: String, turtleId: Int, top: Int, position: Int, temperature: Double, qualite: Double, deltaTop: Int): Unit = {
    println(course, turtleId, top, position, temperature, qualite, deltaTop)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("TurtlePredictions-" + compte)
      val sc = new SparkContext(conf)

      if (args.length == 7) {
        val course: String = args(0)
        val turtleId: Int = args(1).toInt
        val top: Int = args(2).toInt
        val position: Int = args(3).toInt
        val temperature: Double = args(4).toDouble
        val qualite: Double = args(5).toDouble
        val deltaTop: Int = args(6).toInt

        displayValues(course, turtleId, top, position, temperature, qualite, deltaTop)

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
