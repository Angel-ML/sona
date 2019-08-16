/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.spark.angel.ml.optim

import org.apache.spark.angel.ml.linalg
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.angel.ml.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for optimization problem solvers.
 */
@DeveloperApi
trait Optimizer extends Serializable {

  /**
   * Solve the provided convex optimization problem.
   */
  def optimize(data: RDD[(Double, linalg.Vector)], initialWeights: linalg.Vector): linalg.Vector
}
