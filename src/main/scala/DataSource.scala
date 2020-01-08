package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String, targetEntityTypes: Set[String], eventNames: Set[String]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User(role = properties.getOpt[String]("role"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: Map[String, RDD[(String, Item)]] = dsp.targetEntityTypes.map { entityType =>
      (entityType, PEventStore.aggregateProperties(
          appName = dsp.appName,
          entityType = entityType
        )(sc).map { case (entityId, properties) =>
          val item = try {
            // Assume categories is optional property of item.
            Item(categories = properties.getOpt[List[String]]("categories"))
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

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(dsp.eventNames.toList))(sc)
      .cache()

    val d = new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      events = eventsRDD
    )
    logger.info("WOW " + d)
    d
  }
}

case class User(role: Option[String] = None)

case class Item(categories: Option[List[String]])

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
