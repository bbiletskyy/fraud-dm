package fraud.main

import org.apache.spark.SparkConf
import akka.actor.Actor.Receive
import akka.actor.{ Actor, ActorIdentity, Identify, Props }
import java.util.concurrent.Executors
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver._
import scala.concurrent.Await
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayesModel
import spray.json._
import fraud.main.EventJsonProtocol._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.ml.Model
import org.apache.spark.mllib.classification.ClassificationModel

/** Object in charge of running real stream analytics */
object Spark {
  def init(driverHost: String, driverPort: Int, receiverActorName: String) = {
    val conf = sparkConf(driverHost, driverPort)
    val sc = new SparkContext(conf)

    val model = train("fraud_dm", "training_set", sc)

    //    val ssc = new StreamingContext(sc, Seconds(1))
    //    val actorStream = ssc.actorStream[Event](Props[Receiver], receiverActorName)
    //    val fraudEvents = actorStream.filter(e => e.action == "Click").filter(t => isFraud(t, model)).map(t => (t.id, t.toJson.compactPrint))
    //    fraudEvents.saveToCassandra("fraud_dm", "fraud_events", SomeColumns("event_id", "event"))
    val ssc = setupPredictionStream(sc, receiverActorName, model)

    ssc.start()
    ssc.awaitTermination(10000)
    SparkEnv.get.actorSystem
  }

  /** Trains and returns a Naive Bayes model given Cassandra keysapce name and the table name where the training set is available.*/
  def train(keySpace: String, table: String, sc: SparkContext): ClassificationModel = {
    val trainingSet = sc.cassandraTable(keySpace, table)
    val lps = trainingSet.map { r => LabeledPoint(r.getDouble("class_id"), Vectors.dense(r.getDouble("item_id"), r.getDouble("user_id"))) }
    NaiveBayes.train(lps, lambda = 1.0)
  }

  def setupPredictionStream(sc: SparkContext, receiverActorName: String, model: ClassificationModel): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(1))
    val actorStream = ssc.actorStream[Event](Props[Receiver], receiverActorName)
    val fraudEvents = actorStream.filter(e => e.action == "Click").filter(e => isFraud(e, model)).map(e => (e.id, e.toJson.compactPrint))
    fraudEvents.saveToCassandra("fraud_dm", "fraud_events", SomeColumns("event_id", "event"))
    ssc;
  }

  /** Predicts whether an event is a fraud. Returns <code>true</code> if event is fraud given the model, else returns <code>false</code>. */
  def isFraud(event: Event, model: ClassificationModel): Boolean = {
    model.predict(Domain.features(event)) == 1.0
  }

  /** Returns Spark configuration */
  def sparkConf(driverHost: String, driverPort: Int) = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("Spark Streaming with Scala and Akka")
    .set("spark.logConf", "true")
    .set("spark.driver.port", driverPort.toString)
    .set("spark.driver.host", driverHost)
    .set("spark.akka.logLifecycleEvents", "true")
    .set("spark.cassandra.connection.host", "127.0.0.1")
}

/** This actor is a bridge to Spark. It receives events and puts them to the spark stream */
class Receiver extends Actor with ActorHelper {
  override def preStart() = {
    println(s"Starting Spark event Receiver actor at ${context.self.path}")
  }
  def receive = {
    case t: Event =>
      store(t)
  }
}
