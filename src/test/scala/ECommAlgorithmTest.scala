package org.example.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.apache.predictionio.data.storage.{BiMap, Event}
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

class ECommAlgorithmTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-app",
    unseenOnly = true,
    seenEvents = List("buy", "view"),
    similarEvents = List("view"),
    defaultModelEvents = Set("buy"),
    alsModelEvents = Set("view"),
    entityType = "user",
    targetEntityType = "user",
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3),
    roles = Set()
  )
  val algorithm = new ECommAlgorithm(algorithmParams)

  val userStringIntMap = BiMap(Map("u0" -> 0, "u1" -> 1))

  val itemStringIntMap = BiMap(Map("i0" -> 0, "i1" -> 1, "i2" -> 2))

  val users = Map("u0" -> User(), "u1" -> User())


  val i0 = Item(categories = Some(List("c0", "c1")))
  val i1 = Item(categories = None)
  val i2 = Item(categories = Some(List("c0", "c2")))

  val items = Map(
    "i0" -> i0,
    "i1" -> i1,
    "i2" -> i2
  )

  val view = Seq(
    Event(event = "view", entityId = "u0", targetEntityId = Some("i0"), entityType = "user"),
    Event(event = "view", entityId = "u0", targetEntityId = Some("i1"), entityType = "user"),
    Event(event = "view", entityId = "u0", targetEntityId = Some("i1"), entityType = "user"),
    Event(event = "view", entityId = "u1", targetEntityId = Some("i1"), entityType = "user"),
    Event(event = "view", entityId = "u1", targetEntityId = Some("i2"), entityType = "user")
  )

  val buy = Seq(
    Event(event = "buy", entityId = "u0", targetEntityId = Some("i0"), entityType = "user"),
    Event(event = "buy", entityId = "u0", targetEntityId = Some("i1"), entityType = "user"),
    Event(event = "buy", entityId = "u1", targetEntityId = Some("i1"), entityType = "user")
  )


  "ECommAlgorithm.genMLlibRating()" should "create RDD[MLlibRating] correctly" in {

    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = Map("item" -> sc.parallelize(items.toSeq)),
      events = sc.parallelize(view ++ buy)
    )

    val mllibRatings = algorithm.genMLlibRating(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = preparedData
    )

    val expected = Seq(
      MLlibRating(0, 0, 1),
      MLlibRating(0, 1, 2),
      MLlibRating(1, 1, 1),
      MLlibRating(1, 2, 1)
    )

    mllibRatings.collect should contain theSameElementsAs expected
  }

  "ECommAlgorithm.trainDefault()" should "return popular count for each item" in {
    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = Map("item" -> sc.parallelize(items.toSeq)),
      events = sc.parallelize(view ++ buy)
    )

    val popCount = algorithm.trainDefault(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = preparedData
    )

    val expected = Map(0 -> 1, 1 -> 2)

    popCount should contain theSameElementsAs expected
  }

  "ECommAlgorithm.predictKnownuser()" should "return top item" in {

    val top = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((2, 7.5), (0, 5.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictDefault()" should "return top item" in {

    val top = algorithm.predictDefault(
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = None,
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((1, 4.0), (0, 3.0), (2, 1.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictSimilar()" should "return top item" in {

    val top = algorithm.predictSimilar(
      recentFeatures = Vector(Array(1.0, 2.0, 0.5), Array(1.0, 0.2, 0.3)),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = "u0",
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((0, 1.605), (2, 1.525))

    top(0)._1 should be (expected(0)._1)
    top(1)._1 should be (expected(1)._1)
    top(0)._2 should be (expected(0)._2 +- 0.001)
    top(1)._2 should be (expected(1)._2 +- 0.001)
  }
}
