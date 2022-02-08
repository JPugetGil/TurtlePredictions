package entity.behavior

import entity.behavior.BehaviorFormatter.printRegular

case class TurtleRegularData(speed: Int) extends TurtleBehaviorData {
  override def rawData: String = printRegular(speed)
}
