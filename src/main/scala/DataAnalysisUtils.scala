import entity.TurtleJourneyStepEntity
import org.apache.spark.rdd.RDD

object DataAnalysisUtils {

  def turtlesAnalysis(turtlesJourney: RDD[(String, List[TurtleJourneyStepEntity])]): Unit = {
    turtlesJourney.foreach(turtleJourney => {
      turtleAnalysis(turtleJourney)
    })
  }

  def turtleAnalysis(turtleJourney: (String, List[TurtleJourneyStepEntity])): Unit = {
    if (isRegular(turtleJourney._2)) {
      println("Turtle " + turtleJourney._1 + " is regular")
      // the turtle is regular
      return
    }

    val tirednessInformations = isTired(turtleJourney._2)
    if (tirednessInformations._1) {
      // The turtle is tired
      println("Turtle " + turtleJourney._1 + " is tired")
      return
    }

    val cyclicInformations = isCyclic(turtleJourney._2)
    if (cyclicInformations._1) {
      // The turtle is cyclic
      println("Turtle " + turtleJourney._1 + " is cyclic")
      return
    }

    println("Turtle " + turtleJourney._1 + " is lunatic")
  }

  def isRegular(steps: List[TurtleJourneyStepEntity]): Boolean = {
    steps.forall(_.vitesse == steps.head.vitesse)
  }

  def isTired(steps: List[TurtleJourneyStepEntity]): (Boolean, Int, Int) = {
    val maxSpeed = steps.reduce(computeMaxSpeed)
    val maxIndex = steps.indexWhere(element => element.vitesse == maxSpeed.vitesse)
    val minSpeed = steps.reduce(computeMinSpeed)
    val minIndex = steps.indexWhere(element => element.vitesse == minSpeed.vitesse)
    var rhythm = 0

    if (maxIndex > minIndex) {
      // Cas où la vitesse va grandir au fur et à mesure
      rhythm = steps(minIndex + 1).vitesse - steps(minIndex).vitesse
      for (a <- minIndex until maxIndex) {
        if ((steps(a + 1).vitesse - steps(a).vitesse) != rhythm) {
          return (false, 0, 0)
        }
      }
    } else {
      // Cas où la vitesse va diminuer au fur et à mesure
      rhythm = steps(maxIndex).vitesse - steps(maxIndex + 1).vitesse
      for (a <- maxIndex until minIndex - 1) {
        if ((steps(a).vitesse - steps(a + 1).vitesse) != rhythm) {
          return (false, 0, 0)
        }
      }
    }
    (true, maxSpeed.vitesse, rhythm)
  }

  def isCyclic(steps: List[TurtleJourneyStepEntity]): (Boolean, Int) = {
    // TODO : trouver le modèle qui permet de définir un cycle
    (false, 0)
  }

  def computeMaxSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse > t2.vitesse) t1 else t2
  }

  def computeMinSpeed(t1: TurtleJourneyStepEntity, t2: TurtleJourneyStepEntity): TurtleJourneyStepEntity = {
    if (t1.vitesse < t2.vitesse) t1 else t2
  }
}
