package entity.behavior

import entity.behavior.BehaviorFormatter.printLunaticPartialBehavior


case class TurtleLunaticData(behaviors: Array[TurtleSubBehaviorData]) extends TurtleBehaviorData {
  override def rawData: String = {
    behaviors.map(behavior => {
      printLunaticPartialBehavior(behavior.behaviorId, behavior.startTop, behavior.temperature, behavior.qualite, behavior.behaviorData)
    }).mkString(";")
  }
}
