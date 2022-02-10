package utils

import entity.behavior.TurtleDataBuilder.{CYCLIC, LUNATIC, REGULAR, TIRED}
import entity.behavior.{TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleTiredData}
import entity.{TurtleJourneyStepEntity, TurtleTypeEntity}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object DataAnalysisUtils {
  def turtleAnalysis(turtleId: String, turtleJourney: DataFrame): TurtleTypeEntity = {
    val turtleJourneyToRDD = turtleJourney.rdd
      .filter(turtleJ => turtleJ(0) != null && turtleJ(1) != null && turtleJ(2) != null && turtleJ(3) != null && turtleJ(4) != null)
      .map(r => TurtleJourneyStepEntity(
        r(0).asInstanceOf[Int],
        r(1).asInstanceOf[Int],
        r(2).asInstanceOf[Double],
        r(3).asInstanceOf[Double],
        r(4).asInstanceOf[Int])
      )
    val turtleJourneyToArray = turtleJourneyToRDD.collect()

    // (Boolean: isRegular, Int: Constante, Int: Numero de top en echec)
    val regularInfo = isRegular(turtleJourneyToArray)
    if (regularInfo._1) {
      // the turtle is regular
      return TurtleTypeEntity(turtleId.toInt, REGULAR, TurtleRegularData(regularInfo._2))
    }

    // (Boolean: isTired, Int: maxSpeed, Int: valeur abs du rythme, Int: numero top du max, Int: Numero de top en echec)
    val tirednessInfo = isTired(turtleJourneyToArray)
    if (tirednessInfo._1) {
      return TurtleTypeEntity(turtleId.toInt, TIRED, TurtleTiredData(tirednessInfo._2, tirednessInfo._3, tirednessInfo._4))
    }

    // (isCyclic, période, Pattern de taille période, Int: Numero de top en echec)
    val cyclicInfo = isCyclic(turtleJourneyToArray)
    if (cyclicInfo._1) {
      return TurtleTypeEntity(turtleId.toInt, CYCLIC, TurtleCyclicData(cyclicInfo._2, cyclicInfo._3, cyclicInfo._4))
    }

    // TODO : Malik - regularInfo._3, tirednessInfo._5, cyclicInfo._5 (informations à comparer et prendre max pour construction
    TurtleTypeEntity(turtleId.toInt, LUNATIC, TurtleLunaticData("", Array(null))) // TODO : donner infos
  }

  /**
   *
   * @param steps voyage de la tortue
   * @return (isRegular, constante, (Numero de top en echec, Temperature, Qualite))
   */
  def isRegular(steps: Array[TurtleJourneyStepEntity]): (Boolean, Int, (Int, Double, Double)) = {
    val firstVitesse = steps.head.vitesse
    val stepsSize = steps.length
    for (count <- 1 until stepsSize) {
      val isContinuous = steps(count).top == steps(count - 1).top + 1
      val hasntSameSpeed = steps(count).vitesse != steps(count - 1).vitesse
      if (isContinuous && hasntSameSpeed && steps(count).vitesse != firstVitesse) {
        return (false, 0, (steps(count).top, steps(count).temperature, steps(count).qualite))
      }
    }

    (true, firstVitesse, (0, 0, 0))
  }

  def isSameAbsoluteValue(value: Int, ref: Int): Boolean = {
    Math.abs(value) == Math.abs(ref)
  }

  /**
   *
   * @param turtleJourney voyage de la tortue
   * @return (isTired, vitesse max, rythme de diminution ou augmentation, (Numero de top en echec, Temperature, Qualite))
   */
  def isTired(turtleJourney: Array[TurtleJourneyStepEntity]): (Boolean, Int, Int, Int, (Int, Double, Double)) = {
    var maxSpeed = Int.MinValue
    var maxIndex = 0
    var rhythm = 0

    for (a <- 1 until turtleJourney.length - 1) {
      if (a == 1) {
        val first = turtleJourney(1).vitesse - turtleJourney(0).vitesse
        val second = turtleJourney(2).vitesse - turtleJourney(1).vitesse
        val third = turtleJourney(3).vitesse - turtleJourney(2).vitesse
        val fourth = turtleJourney(4).vitesse - turtleJourney(3).vitesse
        val seq = Seq(first, second, third, fourth)
        val supposedRhythm = seq.groupBy(identity).mapValues(_.size).maxBy(a => {
          a._2
        })
        rhythm = supposedRhythm._1
      }

      if (rhythm != 0) {
        val isContinuous = turtleJourney(a).top == turtleJourney(a - 1).top + 1
        val wasContinuous = if (a > 2) turtleJourney(a - 1).top == turtleJourney(a - 2).top + 1 else true
        val difference = turtleJourney(a).vitesse - turtleJourney(a - 1).vitesse
        if (isContinuous && wasContinuous) {
          if (!isSameAbsoluteValue(difference, rhythm)) {
            if (turtleJourney(a).vitesse != 0) {
              if (turtleJourney(a).vitesse >= maxSpeed) {
                maxSpeed = turtleJourney(a).vitesse
                maxIndex = turtleJourney(a).top
              } else {
                return (false, 0, 0, 0, (turtleJourney(a).top, turtleJourney(a).temperature, turtleJourney(a).qualite))
              }
            }
          } else {
            if (maxSpeed < turtleJourney(a).vitesse) {
              maxSpeed = turtleJourney(a).vitesse
              maxIndex = turtleJourney(a).top
            }
          }
        }
      } else {
        return (false, 0, 0, 0, (turtleJourney(1).top, turtleJourney(1).temperature, turtleJourney(1).qualite))
      }
    }
    (true, maxSpeed, Math.abs(rhythm), maxIndex, (0, 0, 0))
  }

  /**
   * Une tortue cyclique possède un motif de vitesses dont tous les éléments sont différents.
   * Pour savoir si une tortue est cyclique, on vérifie si une vitesse apparaît 2 fois dans le parcours, si oui on vérifie
   * la vitesse aux index suivants, si elles sont identiques alors c'est un cycle, sinon elle n'est pas cyclique.
   *
   * @param turtleJourney voyage de la tortue
   * @return (isCyclic, taille du cycle, motif du cycle, (Numero de top en echec, Temperature, Qualite))
   */
  def isCyclic(turtleJourney: Array[TurtleJourneyStepEntity]): (Boolean, Int, Array[Int], Int, (Int, Double, Double)) = {
    val vitesseArrayBuffer = ArrayBuffer[Int](turtleJourney.head.vitesse)
    val firstTop = turtleJourney.head.top
    var checkIndex = 0
    var indexHasChanged = false
    for (i <- 1 until turtleJourney.length) {
      val vitesseList = vitesseArrayBuffer.toList

      if (indexHasChanged && vitesseArrayBuffer(checkIndex) != turtleJourney(i).vitesse) {
        if (turtleJourney(i).top == turtleJourney(i - 1).top + 1 && !vitesseList.contains(turtleJourney(i).vitesse)) {
          return (false, 0, null, 0, (turtleJourney(i).top, turtleJourney(i).temperature, turtleJourney(i).qualite))
        }

      } else if (vitesseArrayBuffer(checkIndex) == turtleJourney(i).vitesse) {
        if (turtleJourney(i).top == turtleJourney(i - 1).top + 1) {
          checkIndex = checkIndex + 1
          checkIndex %= vitesseArrayBuffer.size
          indexHasChanged = true
        }
      } else {
        if (turtleJourney(i).top == turtleJourney(i - 1).top + 1) {
          vitesseArrayBuffer.append(turtleJourney(i).vitesse)
        }
      }
    }

    if (indexHasChanged) {
      (true, vitesseArrayBuffer.size, vitesseArrayBuffer.toArray, firstTop, (0, 0, 0))
    } else {
      val step = turtleJourney(vitesseArrayBuffer.size)
      (false, 0, null, 0, (step.top, step.temperature, step.qualite))
    }
  }

  def computeMaxSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse > t2.vitesse) t1 else t2
  }

  def computeMinSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse < t2.vitesse) t1 else t2
  }
}
