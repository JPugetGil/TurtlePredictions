package entity.behavior

import entity.TurtleTypeEntity
import entity.behavior.TurtleDataBuilder._
import org.apache.spark.sql.Row

object BehaviorFormatter {
  def toEntity(row: Row): TurtleTypeEntity = {
    val turtleType = (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int], row(2).asInstanceOf[String])
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
  def printTired(maxSpeed: Int, step: Int): String = {
    s"$maxSpeed:$step"
  }

  /**
    * Écrit les informations du comportement d'une tortue cyclique.
    *
    * @param patternLength Taille du motif de vitesses
    * @param pattern       Motif de vitesses
    * @return informations d'une tortue fatiguée formatées
    */
  def printCyclic(patternLength: Int, pattern: Array[Int]): String = {
    var patternPrint = ""
    for (p <- pattern) {
      patternPrint += ":" + p.toString
    }
    s"$patternLength$patternPrint"
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
  def printLunaticPartialBehavior(behaviorId: Int, startTop: Int, info: Array[String]): String = {
    var toPrint = ""
    behaviorId match {
      case REGULAR =>
        if (info.length == 1) {
          toPrint = printRegular(info(0).toInt)
        } else {
          throw new IllegalArgumentException("A regular turtle need 1 piece of information, see printRegular doc")
        }
      case TIRED =>
        if (info.length == 2) {
          toPrint = printTired(info(0).toInt, info(1).toInt)
        } else {
          throw new IllegalArgumentException("A tired turtle need 2 pieces of information, see printTired doc")
        }
      case CYCLIC =>
        if (info.length == 2) {
          val pattern = info(1).split('-').map(speed => speed.toInt)
          toPrint = printCyclic(info(0).toInt, pattern)
        } else {
          throw new IllegalArgumentException("A cyclic turtle need 2 pieces of information, see printCyclic doc")
        }
      case other => throw new IllegalArgumentException(s"Behavior ID " + other + " is not recognized")
    }
    s"($behaviorId:$startTop:$toPrint);"
  }

}
