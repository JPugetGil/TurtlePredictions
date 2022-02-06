package entity.behavior


case class TurtleLunaticData(raw: String, behaviors: Array[TurtleSubBehaviorData]) extends TurtleBehaviorData {
  override def rawData: String = raw
}
