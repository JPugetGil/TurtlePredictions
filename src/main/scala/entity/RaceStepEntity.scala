package entity

import org.apache.spark.rdd.RDD

case class RaceStepEntity(
     turtles: RDD[TurtleEntity],
     temperature: Double,
     qualite: Double
) {

  def toTuple: (RDD[TurtleEntity], Double, Double) = {
    (turtles, temperature, qualite)
  }
}
