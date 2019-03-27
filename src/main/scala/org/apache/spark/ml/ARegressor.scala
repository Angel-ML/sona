package org.apache.spark.ml

abstract class ARegressor[
    FeaturesType,
    Learner <: ARegressor[FeaturesType, Learner, M],
    M <: ARegressionModel[FeaturesType, M]] extends Predictor[FeaturesType, Learner, M]
  with APredictorParams {

}
