package zte.MBA.fpg

import grizzled.slf4j.Logger
import io.prediction.controller.{P2LAlgorithm, Params}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}

case class AlgorithmParams(
  minsupport: Double,
  numPartition: Int
  ) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): Model = {
    // Simply count number of events
    // and multiple it by the algorithm parameter
    // and store the number as model
//    val count = data.events.count().toInt * ap.mult
//    new Model(mc = count)
    val fpg = new FPGrowth()
      .setMinSupport(ap.minsupport)
      .setNumPartitions(ap.numPartition)
    val model = fpg.run(data.actionWindows)
    val minConfidence = 0.1
    model.generateAssociationRules(minConfidence).map { rule =>
        rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence
    }.coalesce(1).saveAsTextFile("/ZTE_Demo/FPG_result")

    Model(model)

  }

  def predict(model: Model, query: Query): PredictedResult = {
    // Prefix the query with the model data
    val result = "This no direct resukt"
    PredictedResult(p = result)
  }
}

case class Model(
  fpg: FPGrowthModel[String]
  ) extends Serializable {
}