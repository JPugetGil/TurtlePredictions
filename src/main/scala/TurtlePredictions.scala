import entity.{RaceStepEntity, TurtleEntity, TurtleJourneyStepEntity}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

object TurtlePredictions {
  val compte = "p1608911"

  implicit val turtleReads: Reads[TurtleEntity] = (
    (JsPath \ "id").read[Int] and
      (JsPath \ "top").read[Int] and
      (JsPath \ "position").read[Int]
    ) (TurtleEntity.apply _)

  implicit val raceStepReads: Reads[RaceStepEntity] = (
    (JsPath \ "tortoises").read[Seq[TurtleEntity]] and
      (JsPath \ "temperature").read[Double] and
      (JsPath \ "qualite").read[Double]
    ) (RaceStepEntity.apply _)

  def displayValues(course: String, turtleId: Int, top: Int, position: Int, temperature: Double, qualite: Double, deltaTop: Int): Boolean = {
    println(course, turtleId, top, position, temperature, qualite, deltaTop)
    true
  }

  def readJsonValues(initialData: String): List[JsResult[RaceStepEntity]] = {
    val parsed = Json.parse(initialData)
    val jsonList: List[JsValue] = parsed.as[List[JsValue]]

    jsonList.map(element => {
      println(element.toString())
      element.validate[RaceStepEntity]
    })
  }

  def journeyOfTurtleN(turtleId: Int, data: List[JsResult[RaceStepEntity]]): List[TurtleJourneyStepEntity] = {
    data.map(raceStep => {
      val raceStepEntity = raceStep.get
      val currentTurtle = raceStepEntity.turtles(turtleId)
      TurtleJourneyStepEntity(id = currentTurtle.id, top = currentTurtle.top, position = currentTurtle.position, temperature = raceStepEntity.temperature, qualite = raceStepEntity.qualite)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("TurtlePredictions-" + compte)
      val spark = SparkSession
        .builder()
        .appName("TurtlePredictions")
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
