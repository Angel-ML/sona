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
