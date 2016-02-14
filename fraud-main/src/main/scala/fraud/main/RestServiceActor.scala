package fraud.main

import akka.actor.{ Actor, ActorRef }
import com.datastax.driver.core._
import spray.http.MediaTypes.{ `application/json`, `text/html` }
import spray.httpx.SprayJsonSupport.{ sprayJsonMarshaller, sprayJsonUnmarshaller }
import spray.json.JsonParser
import spray.routing._
import scala.collection.JavaConversions._
import scala.xml.Elem

import spray.json._
import fraud.main.EventJsonProtocol._

/** Actor Service that  is responsible of serving REST calls */
class RestServiceActor(connector: ActorRef) extends Actor with RestService {
  def actorRefFactory = context
  def receive = runRoute(route)
  def communicate(e: Event) = connector ! e
  override def preStart() = println(s"Starting rest-service actor at ${context.self.path}")
  val session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("fraud_dm")
  def cleanDetectedFraudEvents() = session.execute("TRUNCATE fraud_events")
  def detectedFaudEvents() = session.execute("select * from fraud_events")
  def indexHtml(): Elem =
    <html>
      <body>
        <h1>Fraud Event Detection REST API</h1>
        <a href="/fraud">Detected Fraud Events</a>
        <br/>
        <a href="/events">Random Events</a>
      </body>
    </html>

  def cleanHtml(): Elem = {
    cleanDetectedFraudEvents()
    <html>
      <body>
        <h1>Detected fraud events have been cleaned</h1>
        <a href="/">Back</a>
      </body>
    </html>
  }
  def fraudHtml(): Elem =
    <html>
      <body>
        <h1>Detected Fraud Events</h1>
        <a href="/clean">Clean</a>
				<a href="/">Back</a>
        <p>{ detectedFaudEvents().iterator().map(e => <p>{ e.getString("event") }</p>) }</p>
      </body>
    </html>
  
  def randomHtml(): Elem =
    <html>
      <body>
        <h1>Random Events</h1>
        <a href="/">Back</a>
        <p>{ Array.fill[Event](10)(RandomEvent()).map(e => <p>{ e.toJson.prettyPrint }</p>) }</p>
      </body>
    </html>
}

/** This trait defines the routing routines. */
trait RestService extends HttpService {
  import EventJsonProtocol._
  //Propagate posted event for fraud detecytion
  def communicate(t: Event)
  //Show index page
  def indexHtml(): Elem
  //Clean detected farud events
  def cleanHtml(): Elem 
  //Show detected fraud events
  def fraudHtml(): Elem
  //Show random events
  def randomHtml(): Elem

  val route =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            indexHtml()
          }
        }
      }
    } ~ path("event") {
      post {
        entity(as[Event]) { event =>
          complete {
            communicate(event)
            event
          }
        }
      }
    } ~ path("events") {
      get {
        respondWithMediaType(`text/html`) {
          complete { 
            randomHtml()
          }
        }
      }
    } ~ path("fraud") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            fraudHtml()
          }
        }
      }
    } ~ path("clean") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            cleanHtml()
          }
        }
      }
    }
}