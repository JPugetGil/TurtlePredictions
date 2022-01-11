package entity

case class TurtleEntity(
    id: Int,
    top: Int,
    position: Int
) {

  def toTuple: (Int, Int, Int) = {
    (id, top, position)
  }
}
