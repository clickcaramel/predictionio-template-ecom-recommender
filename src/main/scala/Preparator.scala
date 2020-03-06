package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.predictionio.data.storage.Event
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  override
  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      events = trainingData.events)
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: Map[String, RDD[(String, Item)]],
  val events: RDD[Event]
) extends Serializable
