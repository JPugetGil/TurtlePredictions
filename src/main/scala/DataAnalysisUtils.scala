import entity.behavior.BehaviorFormatter.{printCyclic, printRegular}
import entity.behavior.TurtleDataBuilder.{CYCLIC, LUNATIC, REGULAR, TIRED, buildLunatic}
import entity.behavior.{TurtleCyclicData, TurtleLunaticData, TurtleRegularData, TurtleTiredData}
import entity.{TurtleJourneyStepEntity, TurtleTypeEntity}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer


object DataAnalysisUtils {

  def turtleAnalysis(turtleId: String, turtleJourney: DataFrame): TurtleTypeEntity = {
    val regularInfo = isRegular(turtleJourney)
    if (regularInfo._1) {
      println("Turtle " + turtleId + " is regular")
      // the turtle is regular
      return TurtleTypeEntity(turtleId.toInt, REGULAR, TurtleRegularData(regularInfo._2))
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
    val cyclicInfo = isCyclic(turtleJourneyToArray)
    if (cyclicInfo._1) {
      // The turtle is cyclic
      println("Turtle " + turtleId + " is cyclic")
      println("Cycle de taille %d : %s".format(cyclicInfo._2, cyclicInfo._3.mkString("(", ", ", ")")))
      return TurtleTypeEntity(turtleId.toInt, CYCLIC, TurtleCyclicData(cyclicInfo._2, cyclicInfo._3))
    }

    val tirednessInfo = isTired(turtleJourneyToArray)
    if (tirednessInfo._1) {
      // The turtle is tired
      println("Turtle " + turtleId + " is tired")
      return TurtleTypeEntity(turtleId.toInt, TIRED, TurtleTiredData(tirednessInfo._2, tirednessInfo._3))
    }

    println("Turtle " + turtleId + " is lunatic")
    understandLunatic(turtleJourney)
    TurtleTypeEntity(turtleId.toInt, LUNATIC, buildLunatic("")) // TODO : donner infos
  }

  def isRegular(turtleJourney: DataFrame): (Boolean, Int) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("vitesse"))
      .setOutputCol("features")

    val chiSquareTest = ChiSquareTest.test(assembler.transform(turtleJourney), "features", "top")
    (chiSquareTest.select("pValues").head()(0).asInstanceOf[DenseVector](0).==(1.0), turtleJourney.head()(4).asInstanceOf[Int])
  }


  /**
   *
   * @param turtleJourney voyage de la tortue
   * @return (isTired, vitesse max, rythme de diminution ou augmentation)
   */
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

  /**
   * Une tortue cyclique possède un motif de vitesses dont tous les éléments sont différents.
   * Pour savoir si une tortue est cyclique, on vérifie si une vitesse apparaît 2 fois dans le parcours, si oui on vérifie
   * la vitesse aux index suivants, si elles sont identiques alors c'est un cycle, sinon elle n'est pas cyclique.
   *
   * @param turtleJourneyToArray voyage de la tortue
   * @return (isCyclic, taille du cycle, motif du cycle)
   */
  def isCyclic(turtleJourneyToArray: Array[TurtleJourneyStepEntity]): (Boolean, Int, Array[Int]) = {
    val vitesseList = ArrayBuffer[Int](turtleJourneyToArray.head.vitesse)
    var checkIndex = 0
    var indexHasChanged = false
    for (i <- 1 until turtleJourneyToArray.length) {
      if (indexHasChanged && vitesseList(checkIndex) != turtleJourneyToArray(i).vitesse) {
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
      (true, vitesseList.size, vitesseList.toArray)
    } else {
      (false, 0, null)
    }
  }

  def understandLunatic(turtleJourney: DataFrame) = {
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
