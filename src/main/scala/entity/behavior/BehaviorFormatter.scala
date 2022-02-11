package entity.behavior

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder._
import org.apache.spark.sql.Row

object BehaviorFormatter {
  def toEntity(row: Row): TurtleTypeEntity = {
    val turtleType = (row(0).asInstanceOf[String].toInt, row(1).asInstanceOf[String].toInt, row(2).asInstanceOf[String])
    var data: Option[TurtleBehaviorData] = None
    turtleType._2 match {
      case REGULAR =>
        data = Some(buildRegular(turtleType._3))
      case TIRED =>
        data = Some(buildTired(turtleType._3))
      case CYCLIC =>
        data = Some(buildCyclic(turtleType._3))
      case LUNATIC =>
        data = Some(buildLunatic(turtleType._3))
      case other => throw new IllegalArgumentException("Behavior ID " + other + " is not recognized")
    }
    TurtleTypeEntity(turtleType._1, turtleType._2, data.get)
  }

  /**
    * Écrit les informations du comportement d'une tortue régulière.
    *
    * @param speed Vitesse constante de la tortue
    * @return informations d'une tortue régulière formatées
    */
  def printRegular(speed: Int): String = {
    s"$speed"
  }

  /**
    * Écrit les informations du comportement d'une tortue fatiguée.
    *
    * @param maxSpeed Vitesse maximale de la tortue
    * @param step     Différence de vitesse de la tortue entre chaque top
    * @return informations d'une tortue fatiguée formatées
    */
  def printTired(maxSpeed: Int, step: Int, indexMax: Int): String = {
    s"$maxSpeed:$step:$indexMax"
  }

  /**
    * Écrit les informations du comportement d'une tortue cyclique.
    *
    * @param patternLength Taille du motif de vitesses
    * @param pattern       Motif de vitesses
    * @return informations d'une tortue fatiguée formatées
    */
  def printCyclic(patternLength: Int, pattern: Array[Int], indexFirst: Int): String = {
    var patternPrint = ""
    for (p <- pattern) {
      patternPrint += "-" + p.toString
    }
    s"$patternLength:${patternPrint.stripPrefix("-")}:$indexFirst"
  }

  /**
    * Écrit les informations pour 1 type de "sous-comportement" d'une tortue lunatique.
    *
    * Pour le cas d'un comportement cyclique, le motif doit être passé sous le format suivant :
    * "12-78-4-32"
    *
    * @param behaviorId Identifiant du "sous-comportement" : REGULAR = 0, TIRED = 1, CYCLIC = 2
    * @param startTop   Top de début du "sous-comportement"
    * @param info       Informations du comportement, sous forme de string, mêmes informations que pour printRegular(),
    *                   printTired() et printCyclic()
    * @return informations d'un comportement de tortue lunatique formatées
    * @throws IllegalArgumentException Quand la taille de info est incorrecte
    */
  def printLunaticPartialBehavior(behaviorId: Int, startTop: Int, temperature: Double, qualite: Double, info: TurtleBehaviorData): String = {
    s"($behaviorId:$startTop:$temperature:$qualite:${info.rawData})"
  }

}
