package entity.behavior
/**
  * Modélise un "sous-comportement" de tortue lunatique (régulière, fatiguée ou cyclique)/
  * @param behaviorId type de comportement
  * @param startTop top de début du comportement
  * @param behaviorData données du comportement
  */
case class TurtleSubBehaviorData(behaviorId: Int, startTop: Int, temperature: Double, qualite: Double, behaviorData: TurtleBehaviorData) {

}
