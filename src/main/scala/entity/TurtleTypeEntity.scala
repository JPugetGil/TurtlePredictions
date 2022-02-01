package entity

/**
  *
  * @param id Id de la tortue
  * @param behavior 0 =  regular, 1 = tired, 2 = cyclic, 3 = lunatic
  * @param info Info concernant le comportement de la tortue (behavior)
  */
case class TurtleTypeEntity(
    id: String,
    behavior: Int,
    info: String
) {

  def toTuple: (String, Int, String) = {
    (id, behavior, info)
  }

  override def toString: String = {
    id + "," + behavior.toString + "," + info
  }
}
