package transactions


import fraud.main.RandomEvent
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import spray.json._
import fraud.main.EventJsonProtocol._
import scala.concurrent.duration._


class RandomEventSimulation extends Simulation{

  val httpConf = http.baseURL("http://localhost:8080")

  val scn = scenario("Posting Random Events").repeat(1000,"event_nr") {

    exec{session=>
      var event = if(session("event_nr").as[Int]%100==0) RandomEvent.randomFraudEvent() else RandomEvent()
      session.set("event",event.toJson.compactPrint)}.
      exec(http("Posting a random event")
      .post("/event").body(StringBody("${event}")).asJSON)
      .pause(5 millis)
  }

  setUp(scn.inject(atOnceUsers(10))).protocols(httpConf)
}
