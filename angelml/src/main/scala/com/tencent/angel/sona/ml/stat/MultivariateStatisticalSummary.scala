package com.tencent.angel.sona.ml.stat

import org.apache.spark.linalg

/**
 * Trait for multivariate statistical summary of a data matrix.
 */
trait MultivariateStatisticalSummary {

  /**
   * Sample mean vector.
   */

  def mean: linalg.Vector

  /**
   * Sample variance vector. Should return a zero vector if the sample size is 1.
   */

  def variance: linalg.Vector

  /**
   * Sample size.
   */

  def count: Long

  /**
   * Number of nonzero elements (including explicitly presented zero values) in each column.
   */

  def numNonzeros: linalg.Vector

  /**
   * Maximum value of each column.
   */

  def max: linalg.Vector

  /**
   * Minimum value of each column.
   */

  def min: linalg.Vector

  /**
   * Euclidean magnitude of each column
   */

  def normL2: linalg.Vector

  /**
   * L1 norm of each column
   */

  def normL1: linalg.Vector
}
