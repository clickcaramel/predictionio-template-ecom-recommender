package org.example.ecommercerecommendation

import java.sql.{DriverManager, ResultSet}
import java.util.concurrent.TimeUnit

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.{DataMap, EnvironmentFactory, Event, PropertyMap, Storage}
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.json4s.JObject
import org.json4s.native.Serialization

import scala.concurrent.duration.{Duration, TimeUnit}

case class DataSourceParams(appName: String, targetEntityTypes: Set[String], eventNames: Set[String]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Queries, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]
  @transient private implicit lazy val formats = org.json4s.DefaultFormats

  override
  def readTraining(sc: SparkContext): TrainingData = {

    logger.info("Started loading users")
    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = propertiesRdd(sc, "user").map { case (entityId, properties) =>
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
      (entityType, itemsRdd(sc, entityType))
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

  private def propertiesRdd(sc: SparkContext, entityType: String): RDD[(String, PropertyMap)] = {
    val q = if (entityType == "product") {
      s"""
        select
          cast(id as text) as id, 'product/' || id as eventid, '{"categories":[' || COALESCE((SELECT STRING_AGG('"' || category.id || '"', ',') FROM category JOIN product_category_relation AS pcr ON pcr.category_id = category.id WHERE product.id = pcr.product_id), '') || '],"status":"' || published_status || '","language":"english","location":"USA","reward":' || product.fee || '}' as properties,
          GREATEST(updated_at, created_at) as updated_at
        from product
        where published_status = 'published' and id >= ? and id < ?
      """.replace("\n", " ")
    } else if (entityType == "user") {
      s"""
        select
          cast(id as text) as id, 'user/' || id as eventid, '{"role":"' || role || '","categories":[' || COALESCE((SELECT STRING_AGG('"' || category.id || '"', ',') FROM category JOIN product_category_relation AS pcr ON pcr.category_id = category.id JOIN referral_link AS ref ON ref.product_id = pcr.product_id WHERE ref.owner_id = "user".id), '') || ']}' as properties,
        GREATEST(updated_at, created_at) as updated_at
        from "user"
        where id >= ? and id < ?
      """.replace("\n", " ")
    } else {
      s"""
        select
          cast(id as text) as id, 'shop/' || id as eventid, '{"categories":[' || COALESCE((SELECT STRING_AGG('"' || category.id || '"', ',') FROM category JOIN product_category_relation AS pcr ON pcr.category_id = category.id JOIN product AS product ON pcr.product_id=product.id WHERE product.shop_id = shop.id), '') || '],"status":"' || status || '","language":"english","location":"USA","reward":' || shop.fee || '}' as properties,
          GREATEST(updated_at, created_at) as updated_at
        from shop
        where status in ('enabled', 'disconnected') and id >= ? and id < ?
      """.replace("\n", " ")
    }
    val es = Storage.getConfig("PGSQL").get
    val username = es.properties("USERNAME")
    val password = es.properties("PASSWORD")
    val url = es.properties("URL")
    val lower = 0
    val upper = Long.MaxValue
    new JdbcRDD(sc, () => { DriverManager.getConnection(url, username, password) }, q, lower, upper, 1,
      (r: ResultSet) => {
        Event(
          eventId = Option(r.getString("eventid")),
          event = "$set",
          entityType = entityType,
          entityId = r.getString("id"),
          properties = Option(r.getString("properties")).map(x =>
            DataMap(Serialization.read[JObject](x))).getOrElse(DataMap()),
          eventTime = new DateTime(r.getTimestamp("updated_at").getTime, DateTimeZone.forID("UTC"))
        )
      }).map { e =>
      (e.entityId, PropertyMap(e.properties.fields, e.eventTime, e.eventTime))
    }
  }

  private def itemsRdd(sc: SparkContext, entityType: String): RDD[(String, Item)] = {
    propertiesRdd(sc, entityType).map { case (entityId, properties) =>
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
    }.cache()
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
    val timeScore = 1.0 - Math.min(DateTime.now().toDate.getTime - lastUpdated.toDate.getTime, TimeUnit.DAYS.toMillis(90)).toDouble / TimeUnit.DAYS.toMillis(90).toDouble
    val scores = List[Double](
      engineScore,
      if (imageExists) 1.0 else 0.0,
      if (status.exists(s => s == "enabled" || s == "published")) 1.0 else 0.0,
      reward
    )
    (timeScore * 5.0 + scores.fold(0.0)(_+_)) / (scores.size.toDouble + 5.0)
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
