/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.angel.ml.regression

import org.apache.spark.angel.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.angel.ml.linalg.Vector
import org.apache.spark.annotation.DeveloperApi


/**
 * Single-label regression
 *
 * @tparam FeaturesType  Type of input features.
 * @tparam Learner  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
private[spark] abstract class Regressor[
    FeaturesType,
    Learner <: Regressor[FeaturesType, Learner, M],
    M <: RegressionModel[FeaturesType, M]]
  extends Predictor[FeaturesType, Learner, M] with PredictorParams {

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a `Regressor`.
 *
 * @tparam FeaturesType  Type of input features.
 * @tparam M  Concrete Model type.
 */
@DeveloperApi
abstract class RegressionModel[FeaturesType, M <: RegressionModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with PredictorParams {

  // TODO: defaultEvaluator (follow-up PR)
}
