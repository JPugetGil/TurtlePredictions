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

    val regularInfo = isRegular(turtleJourneyToArray)
    if (regularInfo._1) {
      // the turtle is regular
      return TurtleTypeEntity(turtleId.toInt, REGULAR, TurtleRegularData(regularInfo._2))
    }

    val tirednessInfo = isTired(turtleJourneyToArray)
    if (tirednessInfo._1) {
      return TurtleTypeEntity(turtleId.toInt, TIRED, TurtleTiredData(tirednessInfo._2, tirednessInfo._3, tirednessInfo._4))
    }

    // (isCyclic, période, Pattern de taille période)
    val cyclicInfo = isCyclic(turtleJourneyToArray)
    if (cyclicInfo._1) {
      return TurtleTypeEntity(turtleId.toInt, CYCLIC, TurtleCyclicData(cyclicInfo._2, cyclicInfo._3, cyclicInfo._4))
    }

    //    understandLunatic(turtleJourney)
    TurtleTypeEntity(turtleId.toInt, LUNATIC, TurtleLunaticData("", Array(null))) // TODO : donner infos
  }

  def isRegular(steps: Array[TurtleJourneyStepEntity]): (Boolean, Int) = {
    val firstVitesse = steps.head.vitesse
    val stepsSize = steps.length
    for (count <- 1 until stepsSize) {
      val isContinuous = steps(count).top == steps(count - 1).top + 1
      val hasntSameSpeed = steps(count).vitesse != steps(count - 1).vitesse
      if (isContinuous && hasntSameSpeed && steps(count).vitesse != firstVitesse) {
        return (false, 0)
      }
    }

    (true, firstVitesse)
  }

  def isSameAbsoluteValue(value: Int, ref: Int): Boolean = {
    Math.abs(value) == Math.abs(ref)
  }

  /**
   *
   * @param turtleJourney voyage de la tortue
   * @return (isTired, vitesse max, rythme de diminution ou augmentation)
   */
  def isTired(turtleJourney: Array[TurtleJourneyStepEntity]): (Boolean, Int, Int, Int) = {
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
                maxIndex = a
              } else {
                return (false, 0, 0, 0)
              }
            }
          } else {
            if (maxSpeed < turtleJourney(a).vitesse) {
              maxSpeed = turtleJourney(a).vitesse
              maxIndex = a
            }
          }
        }
      } else {
        return (false, 0, 0, 0)
      }
    }
    (true, maxSpeed, Math.abs(rhythm), maxIndex)
  }

  /**
   * Une tortue cyclique possède un motif de vitesses dont tous les éléments sont différents.
   * Pour savoir si une tortue est cyclique, on vérifie si une vitesse apparaît 2 fois dans le parcours, si oui on vérifie
   * la vitesse aux index suivants, si elles sont identiques alors c'est un cycle, sinon elle n'est pas cyclique.
   *
   * @param turtleJourneyToArray voyage de la tortue
   * @return (isCyclic, taille du cycle, motif du cycle)
   */
  def isCyclic(turtleJourneyToArray: Array[TurtleJourneyStepEntity]): (Boolean, Int, Array[Int], Int) = {
    val vitesseArrayBuffer = ArrayBuffer[Int](turtleJourneyToArray.head.vitesse)
    val firstTop = turtleJourneyToArray.head.top
    var checkIndex = 0
    var indexHasChanged = false
    for (i <- 1 until turtleJourneyToArray.length) {
      val vitesseList = vitesseArrayBuffer.toList

      if (indexHasChanged && vitesseArrayBuffer(checkIndex) != turtleJourneyToArray(i).vitesse) {
        if (turtleJourneyToArray(i).top == turtleJourneyToArray(i - 1).top + 1 && !vitesseList.contains(turtleJourneyToArray(i).vitesse)) {
          return (false, 0, null, 0)
        }

      } else if (vitesseArrayBuffer(checkIndex) == turtleJourneyToArray(i).vitesse) {
        if (turtleJourneyToArray(i).top == turtleJourneyToArray(i - 1).top + 1) {
          checkIndex = checkIndex + 1
          checkIndex %= vitesseArrayBuffer.size
          indexHasChanged = true
        }
      } else {
        if (turtleJourneyToArray(i).top == turtleJourneyToArray(i - 1).top + 1) {
          vitesseArrayBuffer.append(turtleJourneyToArray(i).vitesse)
        }
      }
    }

    if (indexHasChanged) {
      (true, vitesseArrayBuffer.size, vitesseArrayBuffer.toArray, firstTop)
    } else {
      (false, 0, null, 0)
    }
  }

  def understandLunatic(turtleJourney: DataFrame): Unit = {
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
  }

  def computeMaxSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse > t2.vitesse) t1 else t2
  }

  def computeMinSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse < t2.vitesse) t1 else t2
  }
}
