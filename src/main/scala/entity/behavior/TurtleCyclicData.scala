package entity.behavior

import entity.behavior.BehaviorFormatter.printCyclic

case class TurtleCyclicData(patternLength: Int, pattern: Array[Int]) extends TurtleBehaviorData {
  override def rawData: String = printCyclic(patternLength, pattern)
}

