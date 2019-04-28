package org.apache.spark.angelml.stat

import org.apache.spark.annotation.Since
import org.apache.spark.angelml.linalg.Vector

/**
 * Trait for multivariate statistical summary of a data matrix.
 */
@Since("1.0.0")
trait MultivariateStatisticalSummary {

  /**
   * Sample mean vector.
   */
  @Since("1.0.0")
  def mean: Vector

  /**
   * Sample variance vector. Should return a zero vector if the sample size is 1.
   */
  @Since("1.0.0")
  def variance: Vector

  /**
   * Sample size.
   */
  @Since("1.0.0")
  def count: Long

  /**
   * Number of nonzero elements (including explicitly presented zero values) in each column.
   */
  @Since("1.0.0")
  def numNonzeros: Vector

  /**
   * Maximum value of each column.
   */
  @Since("1.0.0")
  def max: Vector

  /**
   * Minimum value of each column.
   */
  @Since("1.0.0")
  def min: Vector

  /**
   * Euclidean magnitude of each column
   */
  @Since("1.2.0")
  def normL2: Vector

  /**
   * L1 norm of each column
   */
  @Since("1.2.0")
  def normL1: Vector
}
