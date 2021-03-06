package org.example.ecommercerecommendation

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration

import collection.JavaConverters._

case class ECommAlgorithmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  defaultModelEvents: Set[String],
  alsModelEvents: Set[String],
  roles: Set[String] = Set(),
  entityType: String,
  targetEntityType: String,
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]
) extends Params


case class ProductModel(
  item: Item,
  features: Option[Array[Double]], // features by ALS
  countScore: Double // popular count for default score
)

class ECommModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val productModels: Map[Int, ProductModel],
  val userStringIntMap: BiMap[String, Int],
  val itemStringIntMap: BiMap[String, Int]
) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.size}]" +
    s"(${productModels.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2).toString}...)]" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

class ECommAlgorithm(val ap: ECommAlgorithmParams)
  extends P2LAlgorithm[PreparedData, ECommModel, Queries, PredictedResults] {

  @transient lazy val logger = Logger[this.type]

  override
  def train(sc: SparkContext, data: PreparedData): ECommModel = {
    logger.info("Started training a model")
    try {
      val params = ap
      val users = data.users.filter { u => params.roles.isEmpty || u._2.role.exists(r => params.roles.contains(r)) }

      // create User and item's String ID to integer index BiMap
      val userStringIntMap = BiMap.stringInt(users.keys)
      val itemStringIntMap = BiMap.stringInt(data.items(ap.targetEntityType).keys)

      val mllibRatings: RDD[MLlibRating] = genMLlibRating(
        userStringIntMap = userStringIntMap,
        itemStringIntMap = itemStringIntMap,
        data = data
      )

      // seed for MLlib ALS
      val seed = ap.seed.getOrElse(System.nanoTime)

      logger.info("Started training ALS")
      // use ALS to train feature vectors
      val m = ALS.trainImplicit(
        ratings = mllibRatings,
        rank = ap.rank,
        iterations = ap.numIterations,
        lambda = ap.lambda,
        blocks = -1,
        alpha = 1.0,
        seed = seed)
      logger.info("Done training ALS")

      val userFeatures = m.userFeatures.collectAsMap.toMap

      // convert ID to Int index
      val items = data.items(ap.targetEntityType).map { case (id, item) =>
        (itemStringIntMap(id), item)
      }

      // join item with the trained productFeatures
      val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
        items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

      logger.info("Started training default")
      val popularCount = trainDefault(
        userStringIntMap = userStringIntMap,
        itemStringIntMap = itemStringIntMap,
        data = data
      )
      val maxCount = popularCount.values.max.toDouble
      logger.info("Done training default")

      val productModels: Map[Int, ProductModel] = productFeatures
        .map { case (index, (item, features)) =>
          val pm = ProductModel(
            item = item,
            features = features,
            // NOTE: use getOrElse because popularCount may not contain all items.
            countScore = popularCount.getOrElse(index, 0) / maxCount
          )
          (index, pm)
        }

      logger.info("Done training a model")
      new ECommModel(
        rank = m.rank,
        userFeatures = userFeatures,
        productModels = productModels,
        userStringIntMap = userStringIntMap,
        itemStringIntMap = itemStringIntMap
      )
    } catch {
      case ex: Exception => {
        logger.error("Failed to train the model: " + ex.getLocalizedMessage, ex)
        new ECommModel(-1, Map(), Map(), BiMap(Map()), BiMap(Map()))
      }
    }
  }

  /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRating(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {
    val params = ap
    val events = data.events.filter { e => (params.alsModelEvents.isEmpty || params.alsModelEvents.contains(e.event)) && e.targetEntityType.contains(params.targetEntityType) }
    val eventsTuples: RDD[(String, Event)] = events.flatMap(e => e.targetEntityId.map(id => (id, e)))
    val joined = eventsTuples.leftOuterJoin(data.items(params.targetEntityType))
    val mllibRatings = joined
      .map { case (id, (r: Event, i: Option[Item])) =>
        val user = r.entityId
        val item = r.targetEntityId.getOrElse("0")
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(user, -1)
        val iindex = itemStringIntMap.getOrElse(item, -1)
        // adjust the rating based on the reward
        ((uindex, iindex), 1 * i.map(_.adjustRating(1.0)).getOrElse(0.5))
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }
      .cache()

    mllibRatings
  }

  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for item.
    */
  def trainDefault(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): Map[Int, Int] = {
    val params = ap
    val events = data.events.filter { e => (params.defaultModelEvents.isEmpty || params.defaultModelEvents.contains(e.event)) && e.targetEntityType.contains(params.targetEntityType) }
    // count number of buys
    // (item index, count)
    val buyCountsRDD: RDD[(Int, Int)] = events
      .map { r =>
        val user = r.entityId
        val item = r.targetEntityId.getOrElse("0")
        // Convert user and item String IDs to Int index
        val uindex = userStringIntMap.getOrElse(user, -1)
        val iindex = itemStringIntMap.getOrElse(item, -1)

        (uindex, iindex, 1)
      }
      .filter { case (u, i, v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    buyCountsRDD.collectAsMap.toMap
  }

  override
  def predict(model: ECommModel, queries: Queries): PredictedResults = {
    val predictedResults = queries.queries.asScala.par.map { q =>
      predict(model, q)
    }
    PredictedResults(predictedResults.toList)
  }

  def predict(model: ECommModel, query: Query): PredictedResult = {
    if (
      query.targetEntityType != ap.targetEntityType ||
      query.entityType != ap.entityType ||
      ap.roles.nonEmpty && query.roles.asScala.intersect(ap.roles).isEmpty ||
      model.rank < 0
    ) {
      return PredictedResult(query.id, List())
    }

    val userFeatures = model.userFeatures
    val productModels = model.productModels

    // convert whiteList's string ID to integer index
    val whiteList: Set[Int] = query.whiteList.asScala.toSet[String].flatMap(model.itemStringIntMap.get)

    val finalBlackList: Set[Int] = genBlackList(query = query)
      // convert seen Items list from String ID to interger Index
      .flatMap(x => model.itemStringIntMap.get(x))

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex =>
        userFeatures.get(userIndex)
      }
    var topScores: Array[(Int, Double)] = if (userFeature.isDefined && (query.recentItems == null || query.recentItems.size() <= 0)) {
      // the user has feature vector
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList
      )
    } else {
      Array()
    }

    if (topScores.length < query.limit) {
      // check if the user has recent events on some items
      val recentItems: Set[String] = getRecentItems(query)
      val recentList: Set[Int] = recentItems.flatMap (x =>
        model.itemStringIntMap.get(x))

      val recentFeatures: Vector[Array[Double]] = recentList.toVector.flatMap { i =>
        productModels.get(i).flatMap { pm => pm.features }
      }

      if (recentFeatures.nonEmpty) {
        val similarTopScores = predictSimilar(
          recentFeatures = recentFeatures,
          productModels = productModels,
          query = query.copy(limit = query.limit - topScores.length),
          whiteList = whiteList,
          blackList = finalBlackList
        )
        topScores = topScores ++ similarTopScores
      }

      if (topScores.length < query.limit) {
        val defaultTopScores = predictDefault(
          productModels = productModels,
          query = query.copy(limit = query.limit - topScores.length),
          whiteList = whiteList,
          blackList = finalBlackList
        )
        topScores = topScores ++ defaultTopScores
      }
    }

    val itemScores = topScores.map { case (i, s) =>
      new ItemScore(
        // convert item int index back to string ID
        item = model.itemIntStringMap(i),
        score = s
      )
    }

    new PredictedResult(query.id, itemScores.toList)
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[String] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = ap.entityType,
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some(ap.targetEntityType)),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e: Exception => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("items")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.asScala.toSet ++ seenItems ++ unavailableItems
  }

  /** Get recent events of the user on items for recommending similar items */
  def getRecentItems(query: Query): Set[String] = {
    if (query.recentItems != null && query.recentItems.size > 0) {
      return query.recentItems.asScala.toSet
    }
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Exception => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Set[Int],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          statuses = query.statuses.asScala.toSet,
          categories = query.categories.asScala.toSet,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, pm.features.get)
        (i, pm.item.adjustRating(s))
      }
      .filter(_._2 > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.limit + query.offset)(ord).slice(query.offset, query.offset + query.limit).toArray

    topScores
  }

  /** Default prediction when know nothing about the user */
  def predictDefault(
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Set[Int],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val candidates = productModels.par
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          statuses = query.statuses.asScala.toSet,
          categories = query.categories.asScala.toSet,
          whiteList = whiteList,
          blackList = blackList
        )
      }
    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          statuses = query.statuses.asScala.toSet,
          categories = query.categories.asScala.toSet,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        (i, pm.item.adjustRating(pm.countScore))
      }
      .seq


    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.limit + query.offset)(ord).slice(query.offset, query.offset + query.limit).toArray

    topScores
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilar(
    recentFeatures: Vector[Array[Double]],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Set[Int],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          statuses = query.statuses.asScala.toSet,
          categories = query.categories.asScala.toSet,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        val s = recentFeatures.map{ rf =>
          // pm.features must be defined because of filter logic above
          cosine(rf, pm.features.get)
        }.reduce(_ + _)
        (i, pm.item.adjustRating(s))
      }
      .filter(_._2 > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.limit + query.offset)(ord).slice(query.offset, query.offset + query.limit).toArray

    topScores
  }

  private
  def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
    i: Int,
    item: Item,
    statuses: Set[String],
    categories: Set[String],
    whiteList: Set[Int],
    blackList: Set[Int]
  ): Boolean = {
    // can add other custom filtering here
    (whiteList.isEmpty || whiteList.contains(i)) &&
    (statuses.isEmpty || item.status.exists(statuses.contains)) &&
    !blackList.contains(i) &&
    // filter categories
    (categories.isEmpty || item.categories.map { itemCat =>
        // keep this item if has ovelap categories with the query
        !(itemCat.toSet.intersect(categories).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    )

  }

}
