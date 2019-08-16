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
package org.apache.spark.angel.ml.stat

import org.apache.spark.angel.ml.linalg
import org.apache.spark.annotation.Since

/**
 * Trait for multivariate statistical summary of a data matrix.
 */
@Since("1.0.0")
trait MultivariateStatisticalSummary {

  /**
   * Sample mean vector.
   */
  @Since("1.0.0")
  def mean: linalg.Vector

  /**
   * Sample variance vector. Should return a zero vector if the sample size is 1.
   */
  @Since("1.0.0")
  def variance: linalg.Vector

  /**
   * Sample size.
   */
  @Since("1.0.0")
  def count: Long

  /**
   * Number of nonzero elements (including explicitly presented zero values) in each column.
   */
  @Since("1.0.0")
  def numNonzeros: linalg.Vector

  /**
   * Maximum value of each column.
   */
  @Since("1.0.0")
  def max: linalg.Vector

  /**
   * Minimum value of each column.
   */
  @Since("1.0.0")
  def min: linalg.Vector

  /**
   * Euclidean magnitude of each column
   */
  @Since("1.2.0")
  def normL2: linalg.Vector

  /**
   * L1 norm of each column
   */
  @Since("1.2.0")
  def normL1: linalg.Vector
}
