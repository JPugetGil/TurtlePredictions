import entity.{RaceStepEntity, TurtleEntity, TurtleJourneyStepEntity, TurtleTypeEntity}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._
import utils.DataAnalysisUtils

import java.io.File
import scala.collection.mutable.ArrayBuffer


object TurtleAnalysis {
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

  implicit val turtleJourneyStepReads: Reads[TurtleJourneyStepEntity] = (
    (JsPath \ "top").read[Int] and
      (JsPath \ "position").read[Int] and
      (JsPath \ "temperature").read[Double] and
      (JsPath \ "qualite").read[Double] and
      (JsPath \ "vitesse").read[Int]
    ) (TurtleJourneyStepEntity.apply _)

  def getArrayOfPaths(dir: String): Array[Path] = {
    val fs = FileSystem.get(new Configuration())
    val remoteIterator = fs.listFiles(new Path(dir), false)
    val arrayPath = ArrayBuffer[Path]()

    while (remoteIterator.hasNext) {
      arrayPath.append(remoteIterator.next().getPath)
    }
    arrayPath.toArray
  }

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
    var counter = 0
    data.map(raceStep => {
      val raceStepEntity = raceStep.get
      val currentTurtle = raceStepEntity.turtles(turtleId)
      var turtleJourneyStepEntity: TurtleJourneyStepEntity = null

      if (counter == 0) {
        turtleJourneyStepEntity = TurtleJourneyStepEntity(top = currentTurtle.top, position = currentTurtle.position, temperature = raceStepEntity.temperature, qualite = raceStepEntity.qualite, vitesse = 0)
      } else {
        val computedSpeed: Int = currentTurtle.position - data(counter - 1).get.turtles(turtleId).position
        turtleJourneyStepEntity = TurtleJourneyStepEntity(top = currentTurtle.top, position = currentTurtle.position, temperature = raceStepEntity.temperature, qualite = raceStepEntity.qualite, vitesse = computedSpeed)
      }

      counter = counter + 1
      turtleJourneyStepEntity
    })
  }

  def getDataAndComputeAnalysis(directory: String, raceType: String, ss: SparkSession): Boolean = {
    val pathArray = getArrayOfPaths("%s/%s".format(directory, raceType))
    val results = ArrayBuffer[TurtleTypeEntity]()

    println(s"Nombre de fichiers détectés : ${pathArray.length}")
    pathArray.foreach(path => {
      println(s"Lecture du fichier : ${path.getName}")
      val turtleId = StringUtils.substringBetween(path.getName, "-", ".").split("-").last
      val turtleJourney = ss.read.schema(Encoders.product[TurtleJourneyStepEntity].schema).option("header", "true").csv("%s/%s/%s".format(directory, raceType, path.getName))

      results.append(DataAnalysisUtils.turtleAnalysis(turtleId, turtleJourney))
    })

    val rdd = ss.sparkContext.parallelize(results)
    if (FileSystem.get(new Configuration()).exists(new Path("%s/%s-analysis.csv".format(directory, raceType)))) {
      println("L'analyse et la construction du modèle de cette course a déjà été réalisée")
    } else {
      rdd
        .map(a => a.toString)
        .saveAsTextFile("%s/%s-analysis.csv".format(directory, raceType))
    }
    true
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("TurtleAnalysis-" + compte)
      val spark = SparkSession
        .builder()
        .appName("TurtleAnalysis")
        .config(conf)
        .getOrCreate()

      if (args.length == 1) {
        val course: String = args(0)
        getDataAndComputeAnalysis(s"hdfs:///user/$compte/data", course, spark)

      } else {
        println(
          s"Usage: spark-submit --class TurtleAnalysis /home/$compte/TurtlePredictions-assembly-1.0.jar " +
            "small"
        )
      }
    }
  }
}
