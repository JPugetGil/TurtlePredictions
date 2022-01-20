package entity

case class TurtleJourneyStepEntity(
      id: Int,
      top: Int,
      position: Int,
      temperature: Double,
      qualite: Double
) {

  override def toString: String = {
    (id, top, position, temperature, qualite).toString()
  }
}
