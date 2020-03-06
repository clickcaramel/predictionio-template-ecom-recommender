package org.example.ecommercerecommendation

import org.apache.predictionio.controller.LServing

class Serving
  extends LServing[Queries, PredictedResults] {

  override
  def serve(query: Queries,
    predictedResults: Seq[PredictedResults]): PredictedResults = {
    val mergedResults = predictedResults.flatMap { resultsObj =>
      resultsObj.results
    }.groupBy(_.id).map { case(id, results) =>
      PredictedResult(id, results.map(_.itemScores).toList.fold(List())(_ ++ _))
    }
    PredictedResults(mergedResults.toList)
  }
}
