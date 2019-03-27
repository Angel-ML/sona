package org.apache.spark.ml


abstract class ARegressionModel[FeaturesType, M <: ARegressionModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with APredictorParams {
}