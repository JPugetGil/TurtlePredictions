package utils

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder._
import entity.behavior.{TurtleBehaviorData, TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleSubBehaviorData, TurtleTiredData}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object DataPredictionUtils {

  def doTurtlePrediction(
                          turtleTypeEntity: TurtleTypeEntity,
                          top: Int,
                          position1: Int,
                          position2: Int,
                          position3: Int,
                          temperature: Double,
                          qualite: Double,
                          deltaTop: Int,
                          spark: SparkSession
                        ): Int = {
    turtleTypeEntity.behavior match {
      case REGULAR =>
        val regularTurtleInfo = buildRegular(turtleTypeEntity.info.rawData)
        checkIfParamsAreRegular(regularTurtleInfo, position1, position2, position3)
        doRegularPrediction(regularTurtleInfo, deltaTop, position1)
      case TIRED =>
        val tiredTurtleInfo = buildTired(turtleTypeEntity.info.rawData)
        checkIfParamsAreTired(tiredTurtleInfo, position1, position2, position3)
        doTiredPrediction(tiredTurtleInfo, deltaTop, position1, position2, position3)
      case CYCLIC =>
        val cyclicTurtleInfo = buildCyclic(turtleTypeEntity.info.rawData)
        checkIfParamsAreCyclic(cyclicTurtleInfo, position1, position2, position3)
        doCyclicPrediction(cyclicTurtleInfo, deltaTop, position1, position2)
      case LUNATIC =>
        doLunaticPrediction(buildLunatic(turtleTypeEntity.info.rawData), top, deltaTop, position1, position2, position3, temperature, qualite, spark)
    }
  }

  def doRegularPrediction(info: TurtleRegularData, deltaTop: Int, position1: Int): Int = {
    val computedPosition = position1 + info.speed * deltaTop
    println("Position après deltaTop : %s".format(computedPosition))
    computedPosition
  }

  def doTiredPrediction(data: TurtleTiredData, deltaTop: Int, position1: Int, position2: Int, position3: Int): Int = {
    val positionnedPattern = getTiredPositionnedPattern(data, position1, position2, position3)
    val costPattern = positionnedPattern.sum
    val nombreDeFois = deltaTop / positionnedPattern.length
    val nombreElement = deltaTop % positionnedPattern.length
    val computedPosition = costPattern * nombreDeFois + positionnedPattern.splitAt(nombreElement)._1.sum + position1

    println("Position après deltaTop : %s".format(computedPosition))
    computedPosition
  }

  def doCyclicPrediction(data: TurtleCyclicData, deltaTop: Int, position1: Int, position2: Int): Int = {
    val positionnedPattern = getCyclicPositionnedPattern(data.pattern, position1, position2)
    val costPattern = positionnedPattern.sum
    val nombreDeFois = deltaTop / positionnedPattern.length
    val nombreElement = deltaTop % positionnedPattern.length
    val computedPosition = costPattern * nombreDeFois + positionnedPattern.splitAt(nombreElement)._1.sum + position1

    println("Position après deltaTop : %s".format(computedPosition))
    computedPosition
  }

  def doLunaticPrediction(
                           data: TurtleLunaticData,
                           top: Int,
                           deltaTop: Int,
                           position1: Int,
                           position2: Int,
                           position3: Int,
                           temperature: Double,
                           qualite: Double,
                           spark: SparkSession
                         ): Int = {

    val dataFiltered = data.behaviors.map(behavior => {
      (behavior.behaviorId, behavior.temperature, behavior.qualite)
    })
    val dataframe = spark.createDataFrame(dataFiltered).toDF("behaviorId", "temperature", "qualite")
    val features = new VectorAssembler()
      .setInputCols(Array("temperature", "qualite"))
      .setOutputCol("features")

    val lr = new LinearRegression().setLabelCol("behaviorId")
    val pipeline = new Pipeline().setStages(Array(features, lr))
    val model = pipeline.fit(dataframe)
    val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]

    println(s"Model: R = ${linRegModel.coefficients(0)} * temperature  + ${linRegModel.coefficients(1)} * qualite + ${linRegModel.intercept}")
    println(s"Résultat R = ${linRegModel.coefficients(0)} * $temperature + ${linRegModel.coefficients(1)} * $qualite + ${linRegModel.intercept}")

    val supposedBehavior = (linRegModel.coefficients(0) * temperature + linRegModel.coefficients(1) * qualite + linRegModel.intercept).intValue()
    val matchingBehaviors = dataFiltered.filter(d => {
      d._1 == supposedBehavior
    })

    val formattedBehavior = data.behaviors.map(behavior => (behavior.behaviorId, behavior.temperature, behavior.qualite))
    val behaviorInfos = if (matchingBehaviors.length > 0) {
      findBestMatchingBehaviorAndGetInfos(data.behaviors, matchingBehaviors, temperature, qualite)
    } else {
      findBestMatchingBehaviorAndGetInfos(data.behaviors, formattedBehavior, temperature, qualite)
    }

    behaviorInfos.behaviorId match {
      case REGULAR =>
        val regularTurtleInfo = buildRegular(behaviorInfos.behaviorData.rawData)
        doRegularPrediction(regularTurtleInfo, deltaTop, position1)
      case TIRED =>
        val tiredTurtleInfo = buildTired(behaviorInfos.behaviorData.rawData)
        doTiredPrediction(tiredTurtleInfo, deltaTop, position1, position2, position3)
      case CYCLIC =>
        val cyclicTurtleInfo = buildCyclic(behaviorInfos.behaviorData.rawData)
        doCyclicPrediction(cyclicTurtleInfo, deltaTop, position1, position2)
    }
  }

  def checkIfParamsAreRegular(turtleRegularData: TurtleRegularData, position1: Int, position2: Int, position3: Int): Unit = {
    if (position2 - position1 != position3 - position2 || position2 - position1 != turtleRegularData.speed) {
      println("Les paramètres de position1, position2 et position3 renseignés ne respectent pas les propriétés de cette tortue régulière.")
      println("Nous ignorons cette erreur et réalisons la prédiction selon notre analyse.")
    }
  }

  def checkIfParamsAreTired(tiredTurtleInfo: TurtleTiredData, position1: Int, position2: Int, position3: Int): Unit = {
    val tiredPattern = getTiredPattern(tiredTurtleInfo)
    if (!tiredPattern.contains(position2 - position1) || !tiredPattern.contains(position3 - position2)) {
      println("Les paramètres de position1, position2 et position3 renseignés ne respectent pas les propriétés de cette tortue fatiguée.")
      println("Nous ignorons cette erreur et réalisons la prédiction selon notre analyse.")
    }
  }

  def checkIfParamsAreCyclic(cyclicTurtleInfo: TurtleCyclicData, position1: Int, position2: Int, position3: Int): Unit = {
    if (!cyclicTurtleInfo.pattern.contains(position2 - position1) || !cyclicTurtleInfo.pattern.contains(position3 - position2)) {
      println("Les paramètres de position1, position2 et position3 renseignés ne respectent pas les propriétés de cette tortue cyclique.")
      println("Nous ignorons cette erreur et réalisons la prédiction selon notre analyse.")
    }
  }

  def getTiredPattern(data: TurtleTiredData, isDesc: Boolean = true): Array[Int] = {
    val tiredPattern = ArrayBuffer[Int]()
    if (isDesc) {
      for (v <- data.maxSpeed until 0 by -data.step) {
        tiredPattern.append(v)
      }
    }
    for (v <- 0 until data.maxSpeed by data.step) {
      tiredPattern.append(v)
    }
    if (!isDesc) {
      for (v <- data.maxSpeed until 0 by -data.step) {
        tiredPattern.append(v)
      }
    }
    tiredPattern.toArray
  }

  def getTiredPositionnedPattern(data: TurtleTiredData, position1: Int, position2: Int, position3: Int): Array[Int] = {
    val firstSpeed = position2 - position1
    val secondSpeed = position3 - position2

    val tiredPattern = getTiredPattern(data, firstSpeed > secondSpeed)
    val splittedPattern = tiredPattern.splitAt(tiredPattern.indexOf(firstSpeed))
    Array(splittedPattern._2, splittedPattern._1).flatten
  }

  def getCyclicPositionnedPattern(cyclicPattern: Array[Int], position1: Int, position2: Int): Array[Int] = {
    val firstSpeed = position2 - position1
    if (cyclicPattern.contains(firstSpeed)) {
      val splittedPattern = cyclicPattern.splitAt(cyclicPattern.indexOf(firstSpeed))
      Array(splittedPattern._2, splittedPattern._1).flatten
    } else {
      cyclicPattern
    }
  }

  def findBestMatchingBehaviorAndGetInfos(
                                           behaviors: Array[TurtleSubBehaviorData],
                                           matchingBehaviors: Array[(Int, Double, Double)],
                                           temperature: Double,
                                           qualite: Double
                                         ): TurtleSubBehaviorData = {
    val sortedByTemp = matchingBehaviors.sortWith((e1, e2) => Math.abs(e1._2 - temperature) < Math.abs(e2._2 - temperature))
    val sortedByQualite = matchingBehaviors.sortWith((e1, e2) => Math.abs(e1._3 - qualite) < Math.abs(e2._3 - qualite))
    val bestBehavior = matchingBehaviors.map(element => {
      (element, sortedByTemp.indexOf(element) + sortedByQualite.indexOf(element))
    }).sortWith((e1, e2) => e1._2 < e2._2).head._1

    behaviors.find(behavior => {
      behavior.behaviorId == bestBehavior._1 && behavior.temperature == bestBehavior._2 && behavior.qualite == bestBehavior._3
    }).get
  }
}
