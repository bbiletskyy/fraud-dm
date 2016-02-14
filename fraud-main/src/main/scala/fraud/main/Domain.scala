package fraud.main

import org.apache.spark.mllib.linalg.Vectors
import spray.json.DefaultJsonProtocol
import java.util.UUID._

case class Event(id: String, user: String, item: String, action: String, timestamp: String)

object EventJsonProtocol extends DefaultJsonProtocol {
  implicit val EventFormat = jsonFormat5(Event)
}

object Domain {
  val items = Seq("Banner-Cosmetics", "Banner-Car", "Banner-Toy")
  val itemIds = Map(items(0) -> 0, items(1) -> 1, items(2) -> 2)

  val actions = Seq("Click", "Over", "View")
  val actionIds = Map(actions(0) -> 0, actions(1) -> 1, actions(2) -> 2)
  
  def features(e: Event) = Vectors.dense(itemId(e), actionId(e))
  def itemId(e: Event): Int = itemIds(e.item)
  def actionId(e: Event): Int = actionIds(e.action)
}

object RandomEvent {
  val rnd = new scala.util.Random()
  def randomFraudEvent() = Event(randomUUID.toString, randomUUID.toString, Domain.items(1), Domain.actions(0).toString, timestamp)
  //Constructs a random events
  def apply(): Event = Event(randomUUID.toString, randomUUID.toString, randomItem, randomAction, timestamp)
  def randomItem() = Domain.itemIds.keys.toSeq(rnd.nextInt(Domain.itemIds.keys.size))
  def randomAction() = Domain.actionIds.keys.toSeq(rnd.nextInt(Domain.actionIds.keys.size))
  def timestamp() = new java.util.Date().toString()
}
