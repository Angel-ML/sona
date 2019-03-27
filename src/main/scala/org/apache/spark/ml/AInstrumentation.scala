package org.apache.spark.ml

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.util.Utils
import org.json4s.JsonDSL.{map2jvalue, _}
import org.json4s.{JValue, _}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class AInstrumentation[E <: Estimator[_]] private(estimator: E, dataset: RDD[_]) extends Logging {

  private val id = AInstrumentation.counter.incrementAndGet()
  private val prefix = {
    // estimator.getClass.getSimpleName can cause Malformed class name error,
    // call safer `Utils.getSimpleName` instead
    val className = Utils.getSimpleName(estimator.getClass)
    s"$className-${estimator.uid}-${dataset.hashCode()}-$id: "
  }

  init()

  private def init(): Unit = {
    log(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  /**
    * Logs a message with a prefix that uniquely identifies the training session.
    */
  def log(msg: String): Unit = {
    logInfo(prefix + msg)
  }

  /**
    * Logs the value of the given parameters for the estimator being used in this session.
    */
  def logParams(params: Param[_]*): Unit = {
    val pairs: Seq[(String, JValue)] = for {
      p <- params
      value <- estimator.get(p)
    } yield {
      val cast = p.asInstanceOf[Param[Any]]
      p.name -> parse(cast.jsonEncode(value))
    }
    log(compact(render(map2jvalue(pairs.toMap))))
  }

  def logNumFeatures(num: Long): Unit = {
    log(compact(render("numFeatures" -> num)))
  }

  def logNumClasses(num: Long): Unit = {
    log(compact(render("numClasses" -> num)))
  }

  /**
    * Logs the value with customized name field.
    */
  def logNamedValue(name: String, value: String): Unit = {
    log(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Long): Unit = {
    log(compact(render(name -> value)))
  }

  /**
    * Logs the successful completion of the training session.
    */
  def logSuccess(model: Model[_]): Unit = {
    log(s"training finished")
  }
}


object AInstrumentation {
  private val counter = new AtomicLong(0)

  /**
    * Creates an instrumentation object for a training session.
    */
  def create[E <: Estimator[_]](estimator: E, dataset: Dataset[_]): AInstrumentation[E] = {
    create[E](estimator, dataset.rdd)
  }

  /**
    * Creates an instrumentation object for a training session.
    */
  def create[E <: Estimator[_]](estimator: E, dataset: RDD[_]): AInstrumentation[E] = {
    new AInstrumentation[E](estimator, dataset)
  }

}