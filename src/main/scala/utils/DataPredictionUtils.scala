package utils

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder._
import entity.behavior.{TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleTiredData}

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
                          deltaTop: Int
                        ) = {
    turtleTypeEntity.behavior match {
      // FIXME : Adapter en fonction des nouveaux paramètres
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
        doLunaticPrediction(buildLunatic(turtleTypeEntity.info.rawData))
    }
  }


  def doRegularPrediction(info: TurtleRegularData, deltaTop: Int, position1: Int): Int = {
    val computedPosition =  position1 + info.speed * deltaTop
    println("Position après deltaTop : %s".format(computedPosition))
    computedPosition
  }

  def doTiredPrediction(data: TurtleTiredData, deltaTop: Int, position1: Int, position2: Int, position3: Int): Int = {
    // max, step, indexMax
    val tiredPattern = getTiredPattern(data)
    val positionnedPattern = getTiredPositionnedPattern(tiredPattern, position1, position2, position3)
    val costPattern = tiredPattern.sum
    val nombreDeFois = deltaTop / tiredPattern.length
    val nombreElement = deltaTop % tiredPattern.length

    val computedPosition = costPattern * nombreDeFois + tiredPattern.splitAt(nombreElement)._1.sum + position1
    println("Position après deltaTop : %s".format(computedPosition))

    computedPosition // TODO : Test me
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

  def doLunaticPrediction(data: TurtleLunaticData): Int = {
    0 // TODO
  }

  def checkIfParamsAreRegular(turtleRegularData: TurtleRegularData ,position1: Int, position2: Int, position3: Int): Unit = {
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

  def getTiredPattern(data: TurtleTiredData): Array[Int] = {
    val tiredPattern = ArrayBuffer[Int]()
    for (v <- data.maxSpeed until 0 by -data.step) {
      tiredPattern.append(v)
    }
    for (v <- 0 until data.maxSpeed by data.step) {
      tiredPattern.append(v)
    }
    tiredPattern.toArray
  }

  // FIXME : Faire en sorte de générer le pattern adapté à la situation
  def getTiredPositionnedPattern(tiredPattern: Array[Int], position1: Int, position2: Int, position3: Int): Array[Int] = {
    val firstSpeed = position2 - position1
    val secondSpeed = position3 - position2
    if (tiredPattern.contains(firstSpeed) && tiredPattern.contains(secondSpeed)) {
      if (firstSpeed > secondSpeed) {
        // La vitesse décroit
        val splittedPattern = tiredPattern.splitAt(tiredPattern.indexOf(firstSpeed))
        Array(splittedPattern._2, splittedPattern._1).flatten

      } else {
        // La vitesse croit

      }
    } else {
      tiredPattern
    }
    null
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

  /*def understandLunatic(turtleJourney: DataFrame): Unit = {
    // FIXME : Faire le processus de compréhension de la lunatique
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
