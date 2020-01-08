package org.example.ecommercerecommendation

import org.apache.predictionio.controller.EngineFactory
import org.apache.predictionio.controller.Engine

case class Query(
  user: String,
  limit: Int,
  entityType: String = "user",
  targetEntityType: String = "item",
  roles: java.util.Set[String] = new java.util.HashSet(),
  categories: java.util.Set[String] = new java.util.HashSet(),
  whiteList: java.util.Set[String] = new java.util.HashSet(),
  blackList: java.util.Set[String] = new java.util.HashSet(),
  offset: Int = 0
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
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
