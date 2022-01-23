package entity

case class TurtleJourneyStepEntity(
      top: Int,
      position: Int,
      temperature: Double,
      qualite: Double,
      vitesse: Int
) {

  override def toString: String = {
    (top, position, temperature, qualite, vitesse).toString()
  }
}
