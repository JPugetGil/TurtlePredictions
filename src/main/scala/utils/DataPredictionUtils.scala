package utils

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder.{CYCLIC, LUNATIC, REGULAR, TIRED, buildCyclic, buildLunatic, buildRegular, buildTired}
import entity.behavior.{TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleTiredData}

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
    println("Position au top %s : %s".format(position, data.rawData))
    0 // TODO
  }

  def doCyclicPrediction(data: TurtleCyclicData, position: Int): Int = {
    println("Position au top %s : %s".format(position, data.rawData))
    0 // TODO
  }

  def doLunaticPrediction(data: TurtleLunaticData): Int = {
    0 // TODO
  }
}
