package entity.behavior

import scala.collection.mutable.ArrayBuffer

object TurtleDataBuilder {
  final val REGULAR = 0
  final val TIRED = 1
  final val CYCLIC = 2
  final val LUNATIC = 3

  /**
    * Découpe une chaîne de caractères en éléments individuels selon un format défini.
    * @param raw Données de comportement formatées de la tortue
    * @return Array[String]
    */
  def splitElements(raw: String): Array[String] = {
    raw.split(':')
  }

  /**
    * Crée un objet de données pour une tortue régulière à partir de données string formatées.
    * @param raw Données de comportement formatées de la tortue
    * @return Données sous forme d'objet
    */
  def buildRegular(raw: String): TurtleRegularData = {
    TurtleRegularData(raw.toInt)
  }

  /**
    * Crée un objet de données pour une tortue fatiguée à partir de données string formatées.
    * @param raw Données de comportement formatées de la tortue
    * @return Données sous forme d'objet
    */
  def buildTired(raw: String): TurtleTiredData = {
    val rawArray = splitElements(raw)
    if (rawArray.length != 2) {
      throw new IllegalArgumentException()
    }
    TurtleTiredData(rawArray(0).toInt, rawArray(1).toInt)
  }

  /**
    * Crée un objet de données pour une tortue cyclique à partir de données string formatées.
    * @param raw Données de comportement formatées de la tortue
    * @return Données sous forme d'objet
    */
  def buildCyclic(raw: String): TurtleCyclicData = {
    val rawArray = splitElements(raw)
    if (rawArray.length != 2) {
      throw new IllegalArgumentException()
    }
    val length = rawArray(0).toInt
    val patternArray = rawArray(1).split('-').map(speed => speed.toInt)
    TurtleCyclicData(length, patternArray)
  }

  /**
    * Crée un objet de données pour une tortue lunatique à partir de données string formatées.
    *
    * @param raw Données de comportement formatées de la tortue
    * @return Données sous forme d'objet
    */
  def buildLunatic(raw: String): TurtleLunaticData = {
    val rawArray = raw.split(';')
    if (rawArray.length != 3) {
      throw new IllegalArgumentException(s"Raw data doesn't have the right format (given : \"${rawArray.mkString("(", ", ", ")")}\"")
    }
    val behaviors = ArrayBuffer[TurtleSubBehaviorData]()
    rawArray.foreach(b => {
      val bClean = b.stripPrefix("(").stripSuffix(")")
      val bArray = splitElements(bClean)
      val behaviorType = bArray(0).toInt
      val behaviorRawData = bArray.slice(2, bArray.length).mkString(":")
      var data: Option[TurtleBehaviorData] = None
      behaviorType match {
        case REGULAR =>
          data = Some(buildRegular(behaviorRawData))
        case TIRED =>
          data = Some(buildTired(behaviorRawData))
        case CYCLIC =>
          data = Some(buildCyclic(behaviorRawData))
        case other => new IllegalArgumentException(s"Behavior ID \"$other\" is not recognized")
      }
      behaviors.append(new TurtleSubBehaviorData(REGULAR, bArray(1).toInt, data.get))
    })

    TurtleLunaticData(raw, behaviors.toArray)
  }
}
