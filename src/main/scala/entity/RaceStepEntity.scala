package entity

case class RaceStepEntity(
     turtles: Seq[TurtleEntity],
     temperature: Double,
     qualite: Double
) {

  def toTuple: (Seq[TurtleEntity], Double, Double) = {
    (turtles, temperature, qualite)
  }
}

