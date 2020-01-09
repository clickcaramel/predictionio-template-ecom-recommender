package org.example.ecommercerecommendation

import org.apache.predictionio.controller.EngineFactory
import org.apache.predictionio.controller.Engine

case class Query(
  user: String,
  limit: Int,
  entityType: String = "user",
  targetEntityType: String = "item",
  statuses: java.util.Set[String],
  roles: java.util.Set[String],
  categories: java.util.Set[String],
  whiteList: java.util.Set[String],
  blackList: java.util.Set[String],
  offset: Int
) extends Serializable

case class PredictedResult(
  itemScores: List[ItemScore]
) extends Serializable

case class ItemScore(
  item: String,
  score: Double
) extends Serializable

object ECommerceRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
