package entity.behavior

import entity.behavior.BehaviorFormatter.printTired

case class TurtleTiredData(maxSpeed: Int, step: Int) extends TurtleBehaviorData {
  override def rawData: String = printTired(maxSpeed, step)
}
