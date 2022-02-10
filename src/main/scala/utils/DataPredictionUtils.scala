package utils

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder.{CYCLIC, LUNATIC, REGULAR, TIRED, buildCyclic, buildLunatic, buildRegular, buildTired}
import entity.behavior.{TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleTiredData}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object DataPredictionUtils {

  def doTurtlePrediction(turtleTypeEntity: TurtleTypeEntity, position: Int, temp: Double, qual: Double, deltatop: Int): Int = {
    turtleTypeEntity.behavior match {
      case REGULAR =>
        doRegularPrediction(buildRegular(turtleTypeEntity.info.rawData), position)
      case TIRED =>
        doTiredPrediction(buildTired(turtleTypeEntity.info.rawData), position)
      case CYCLIC =>
        doCyclicPrediction(buildCyclic(turtleTypeEntity.info.rawData), position)
      case LUNATIC =>
        doLunaticPrediction(buildLunatic(turtleTypeEntity.info.rawData))
    }
  }

  def doRegularPrediction(info: TurtleRegularData, position: Int): Int = {
    val computedPosition = info.speed * position
    println("Position au top %s : %s".format(position, computedPosition))
    computedPosition
  }

  def doTiredPrediction(data: TurtleTiredData, position: Int): Int = {
    // max, step, indexMax
    val tiredPattern = ArrayBuffer[Int]()
    for (v <- 0 until data.maxSpeed by data.step) {
      tiredPattern.append(v)
    }
    for (v <- data.maxSpeed until 0 by -data.step) {
      tiredPattern.append(v)
    }
    val costPattern = tiredPattern.sum


    0 // TODO
  }

  def doCyclicPrediction(data: TurtleCyclicData, position: Int): Int = {
    println("Position au top %s : %s".format(position, data.rawData))
    0 // TODO
  }

  def doLunaticPrediction(data: TurtleLunaticData): Int = {
    0 // TODO
  }

  /*def understandLunatic(turtleJourney: DataFrame): Unit = {
    // TODO : fixme please
    turtleJourney.show()
    val assembler = new VectorAssembler()
      .setInputCols(Array("vitesse", "qualite", "temperature"))
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features") // setting features column
      .setLabelCol("top")

    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    val lrModel = pipeline.fit(turtleJourney)
    val linRegModel = lrModel.stages(1).asInstanceOf[LinearRegressionModel]

    println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linRegModel.summary.r2}")
  }*/
}
