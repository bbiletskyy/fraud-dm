package fraud.main

import org.apache.spark.mllib.linalg.Vectors
import spray.json.DefaultJsonProtocol
import java.util.UUID._

case class Event(id: String, user: String, item: String, action: String, timestamp: String)

object EventJsonProtocol extends DefaultJsonProtocol {
  implicit val EventFormat = jsonFormat5(Event)
}

object Domain {
  val items = Seq("Toy Banner", "Suit Banner", "Skirt Banner")
  val itemIds = Map(items(0) -> 0, items(1) -> 1, items(2) -> 2)
  
  val actions = Seq("Click", "View", "Loaded")
  
  val users = Seq("Billy", "John", "Mary")
  val userIds = Map(users(0) -> 0, users(1) -> 1, users(2) -> 2)
  
  def features(e: Event) = Vectors.dense(itemId(e), userId(e))
  def itemId(e: Event): Int = itemIds(e.item)
  def userId(e: Event): Int = userIds(e.user)
}

object RandomEvent {
  val rnd = new scala.util.Random()
  def randomFraudEvent() = Event(randomUUID.toString, Domain.users(0).toString, Domain.items(1), Domain.actions(0), timestamp)
  //Constructs a random events
  def apply(): Event = Event(randomUUID.toString, randomUser, randomItem, randomAction, timestamp)
  def randomItem() = Domain.items(rnd.nextInt(Domain.items.size))
  def randomUser() = Domain.users(rnd.nextInt(Domain.users.size))
  def randomAction() = Domain.actions(rnd.nextInt(Domain.actions.size))
  def timestamp() = new java.util.Date().toString()
}
