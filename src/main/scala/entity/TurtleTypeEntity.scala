package entity

import entity.behavior.TurtleBehaviorData

/**
  *
  * @param id       Id de la tortue
  * @param behavior 0 = regular, 1 = tired, 2 = cyclic, 3 = lunatic
  * @param info     Info concernant le comportement de la tortue (behavior)
  */
case class TurtleTypeEntity(
 id: Int,
 behavior: Int,
 info: TurtleBehaviorData
) {

  def toTuple: (Int, Int, String) = {
    (id, behavior, info.rawData)
  }

  override def toString: String = {
    id + "," + behavior.toString + "," + info.rawData
  }
}
