import entity.TurtleJourneyStepEntity
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


object DataAnalysisUtils {

  def turtleAnalysis(turtleId: String, turtleJourney: DataFrame): Unit = {
    if (isRegular(turtleJourney)) {
      println("Turtle " + turtleId + " is regular")
      // the turtle is regular
      return
    }

    val turtleJourneyToRDD = turtleJourney.rdd.map(r => TurtleJourneyStepEntity(
      r(1).asInstanceOf[Int],
      r(1).asInstanceOf[Int],
      r(2).asInstanceOf[Double],
      r(3).asInstanceOf[Double],
      r(4).asInstanceOf[Int])
    )
    val turtleJourneyToArray = turtleJourneyToRDD.collect()

    // (isCyclic, période, Pattern de taille période)
    val cyclicInformations = isCyclic(turtleJourneyToArray)
    if (cyclicInformations._1) {
      // The turtle is cyclic
      println("Turtle " + turtleId + " is cyclic")
      println("Cycle de taille %d : %s".format(cyclicInformations._2, cyclicInformations._3.toString()))
      return
    }

    // FIXME : Test with small and tofix if it bugs
    val tirednessInformations = isTired(turtleJourneyToArray)
    if (tirednessInformations._1) {
      // The turtle is tired
      println("Turtle " + turtleId + " is tired")
      return
    }

    println("Turtle " + turtleId + " is lunatic")
  }

  def isRegular(steps: List[TurtleJourneyStepEntity]): Boolean = {
    steps.forall(_.vitesse == steps.head.vitesse)
  }

  def isRegular(turtleJourney: DataFrame): Boolean = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("vitesse"))
      .setOutputCol("features")

    val chiSquareTest = ChiSquareTest.test(assembler.transform(turtleJourney), "features", "top")
    chiSquareTest.select("pValues").head()(0).asInstanceOf[DenseVector](0).==(1.0)
  }


  def isTired(turtleJourney: Array[TurtleJourneyStepEntity]): (Boolean, Int, Int) = {
    val maxSpeed = turtleJourney.reduce(computeMaxSpeed)
    val maxIndex = turtleJourney.indexWhere(element => element.vitesse == maxSpeed.vitesse)
    val minSpeed = turtleJourney.reduce(computeMinSpeed)
    val minIndex = turtleJourney.indexWhere(element => element.vitesse == minSpeed.vitesse)
    var rhythm = 0

    if (maxIndex > minIndex) {
      // Cas où la vitesse va grandir au fur et à mesure
      rhythm = turtleJourney(minIndex + 1).vitesse - turtleJourney(minIndex).vitesse
      for (a <- minIndex until maxIndex - 1) {
        if ((turtleJourney(a + 1).vitesse - turtleJourney(a).vitesse) != rhythm) {
          return (false, 0, 0)
        }
      }
    } else {
      // Cas où la vitesse va diminuer au fur et à mesure
      rhythm = turtleJourney(maxIndex).vitesse - turtleJourney(maxIndex + 1).vitesse
      for (a <- maxIndex until minIndex - 1) {
        if ((turtleJourney(a).vitesse - turtleJourney(a + 1).vitesse) != rhythm) {
          return (false, 0, 0)
        }
      }
    }
    (true, maxSpeed.vitesse, rhythm)
  }

  def isCyclic(turtleJourneyToArray: Array[TurtleJourneyStepEntity]): (Boolean, Int, List[Int]) = {
    val vitesseList = ArrayBuffer[Int](turtleJourneyToArray.head.vitesse)
    var checkIndex = 0
    var indexHasChanged = false
    for (i <- 1 until turtleJourneyToArray.length) {
      if (indexHasChanged & vitesseList(checkIndex) != turtleJourneyToArray(i).vitesse) {
        return (false, 0, null)
      } else if (vitesseList(checkIndex) == turtleJourneyToArray(i).vitesse) {
        checkIndex = checkIndex + 1
        checkIndex %= vitesseList.size
        indexHasChanged = true
      } else {
        vitesseList.append(turtleJourneyToArray(i).vitesse)
      }
    }

    if (indexHasChanged) {
      (true, vitesseList.size, vitesseList.toList)
    } else {
      (false, 0, null)
    }
  }

  def computeMaxSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse > t2.vitesse) t1 else t2
  }

  def computeMinSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse < t2.vitesse) t1 else t2
  }
}
