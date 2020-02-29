package org.example.ecommercerecommendation

import java.util.concurrent.TimeUnit

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger
import org.joda.time.DateTime

import scala.concurrent.duration.Duration

case class DataSourceParams(appName: String, targetEntityTypes: Set[String], eventNames: Set[String]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Queries, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    logger.info("Started loading users")
    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User(role = properties.getOpt[String]("role").orElse(Some("user")))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()
    logger.info("Loaded users")
    logger.info("Started loading items")

    // create a RDD of (entityID, Item)
    val itemsRDD: Map[String, RDD[(String, Item)]] = dsp.targetEntityTypes.map { entityType =>
      (entityType, PEventStore.aggregateProperties(
          appName = dsp.appName,
          entityType = entityType
        )(sc).map { case (entityId, properties) =>
          val item = try {
            Item(
              imageExists = properties.getOrElse[Boolean]("imageExists", false),
              categories = properties.getOpt[List[String]]("categories"),
              status = properties.getOpt[String]("status"),
              lastUpdated = properties.lastUpdated,
              reward = properties.getOpt[Double]("reward").getOrElse(0.0)
            )
          } catch {
            case e: Exception => {
              logger.error(s"Failed to get properties ${properties} of" +
                s" item ${entityId}. Exception: ${e}.")
              throw e
            }
          }
          (entityId, item)
        }.cache())
    }.toMap
    logger.info("Loaded items")
    logger.info("Started loading events")

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(dsp.eventNames.toList))(sc)
      .cache()
    logger.info("Loaded events")

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      events = eventsRDD
    )
  }
}

case class User(role: Option[String] = None)

case class Item(
     imageExists: Boolean = false,
     categories: Option[List[String]] = None,
     status: Option[String] = None,
     lastUpdated: DateTime = DateTime.now(),
     reward: Double = 0.0
) {
  def adjustRating(engineScore: Double): Double = {
    val scores = List[Double](
      engineScore,
      if (imageExists) 1.0 else 0.0,
      if (status.exists(s => s == "enabled" || s == "published")) 1.0 else 0.0,
      1.0 - Math.min(DateTime.now().toDate.getTime - lastUpdated.toDate.getTime, TimeUnit.DAYS.toMillis(30)).toDouble / TimeUnit.DAYS.toMillis(30).toDouble
    )
    (10.0 * reward + scores.fold(0.0)(_+_)) / (scores.size.toDouble + 10.0)
  }
}

case class ViewEvent(user: String, item: String, t: Long)

case class BuyEvent(user: String, item: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val items: Map[String, RDD[(String, Item)]],
  val events: RDD[Event]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.map(_._2.count())} (${items.take(2).toList}...)]" +
    s"events: [${events.count()}] (${events.take(2).toList}...)"
  }
}
