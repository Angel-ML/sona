package com.tencent.angel.sona.utils

import java.util.Random

import breeze.linalg.{DenseVector, Matrix => BM}
import breeze.plot._
import com.tencent.angel.sona.common.measure._
import com.tencent.angel.sona.common.measure.training._
import org.apache.spark.mllib.linalg.Matrix

object PlotUtils {
  private val sin: Double = Math.sin(Math.PI * 2 / 5)
  private val cos: Double = Math.cos(Math.PI * 2 / 5)

  private def rotate(x: Double, y: Double): (Double, Double) = {
    (x * cos - y * sin, x * sin + y * cos)
  }

  private def lossPlot(p: Plot, lossArray: Array[Double]): Unit = {
    val x = DenseVector[Double](lossArray.indices.toArray.map(_.toDouble))
    val y = DenseVector[Double](lossArray)
    p += plot(x, y)

    p.title = f"Loss Plot"
    p.xlabel = "Epoch"
    p.ylabel = "Loss"
    p.setYAxisDecimalTickUnits()
  }

  private def rocPlot(p: Plot, rocArray: Array[(Double, Double)], auc: Double): Unit = {
    val (fpr, tpr) = rocArray.unzip
    p += plot(DenseVector[Double](fpr), DenseVector[Double](tpr))

    val diam = DenseVector[Double]((0 to 100).toArray.map(x => x / 100.0))
    p += plot(diam, diam, '.')

    p.title = f"ROC Curve(AUC=$auc%.3f)"
    p.xlabel = "1-Specificity (TNR)"
    p.ylabel = "Sensitivity (TPR)"
    p.setXAxisDecimalTickUnits()
    p.setYAxisDecimalTickUnits()
    p.xlim = (0.0, 1.0)
    p.xlim = (0.0, 1.0)
  }

  private def prPlot(p: Plot, prArray: Array[(Double, Double)], auc: Double): Unit = {
    val (recall, precision) = prArray.unzip
    p += plot(DenseVector[Double](recall), DenseVector[Double](precision))

    p.title = f"PR Curve(AUC=$auc%.3f)"
    p.xlabel = "Recall"
    p.ylabel = "Precision"
    p.setXAxisDecimalTickUnits()
    p.setYAxisDecimalTickUnits()
    p.xlim = (0.0, 1.0)
    p.xlim = (0.0, 1.0)
  }

  private def r2Plot(p: Plot, r2: Double, xValue: Array[Double],
                     slope: Double, intercept: Double, rmse: Double): Unit = {
    val yValue = xValue.map { x => slope * x + intercept }

    val rand = new Random()
    val yValueWithErr = yValue.map { x => x + rmse * rand.nextGaussian() }

    val (xMin, xMax) = (xValue.min, xValue.max)
    val (yMin, yMax) = (yValueWithErr.min, yValueWithErr.max)

    p += plot(DenseVector[Double](xValue), DenseVector[Double](yValue))

    val dotSize = 0.3 * Math.min(xMax - xMin, yMax - yMin) / xValue.length
    p += scatter(DenseVector[Double](xValue), DenseVector[Double](yValueWithErr),
      (_: Int) => dotSize)

    p.title = f"R2 Plot(r2=$r2%.3f)"
    p.xlabel = "True Value"
    p.ylabel = "Predicted Value"
    p.setXAxisDecimalTickUnits()
    p.setYAxisDecimalTickUnits()

    val xMargin = (xMax - xMin) * 0.1
    p.xlim = (xMin - xMargin, xMax + xMargin)

    val yMargin = (yMax - yMin) * 0.1
    p.xlim = (yMin - yMargin, yMax + yMargin)
  }

  private def performancePlot(p: Plot, pull: Double, forward: Double,
                              backward: Double, push: Double, update: Double): Unit = {
    var last: (Double, Double) = (0.0, 1.0)
    val dots = (0 until 5).toArray.map { idx =>
      if (idx == 0) {
        last
      } else {
        last = rotate(last._1, last._2)
        last
      }
    }

    val (x, y) = dots.unzip
    val labels = Array("pull", "forward", "backward", "push", "update")
    val times = Array(pull, forward, backward, push, update)
    val sumTime = 1.0 * times.sum
    p += scatter(DenseVector[Double](x), DenseVector[Double](y),
      (i: Int) => times(i) / sumTime,
      labels = (idx: Int) => f"${labels(idx)}(${times(idx) / sumTime * 100}%.2f%%)"
    )

    p.title = "Performance"
    p.xlim(-1.5, 1.5)
    p.ylim(-1.5, 1.5)
    p.xlabel = "pull->forward->backward->push->update"
    p.ylabel = "sum(*)=1"
  }

  private def confusionPlot(p: Plot, confusion: Matrix, acc: Double): Unit = {
    val labelFunc = new PartialFunction[(Int, Int), String] {
      override def isDefinedAt(x: (Int, Int)): Boolean = {
        x._1 >= 0 && x._2 >= 0
      }

      override def apply(v1: (Int, Int)): String = {
        s"(${v1._1}, ${v1._2}): 10"
      }
    }

    val method = confusion.getClass.getMethod("asBreeze")
    method.setAccessible(true)
    val breezeMat = method.invoke(confusion).asInstanceOf[BM[Double]]
    p += image(breezeMat, labels = labelFunc)

    p.title = f"Confusion Matrix(ACC=$acc%.3f)"
    p.xlabel = "True Classes"
    p.ylabel = "Predicted Classes"
  }

  def plotTrainStat(stat: TrainingStat, fileName: String): Unit = {
    val f = Figure()

    stat match {
      case clz: ClassificationTrainingStat =>
        clz.getSummary match {
          case bin: BinaryClassificationSummary =>
            f.height = 1000
            f.width = 1000
            val loss = f.subplot(2, 2, 0)
            lossPlot(loss, clz.getHistLoss)

            val roc = f.subplot(2, 2, 1)
            rocPlot(roc, bin.roc, auc = bin.areaUnderROC)

            val pr = f.subplot(2, 2, 2)
            prPlot(pr, bin.pr, auc = bin.areaUnderPR)

            val performance = f.subplot(2, 2, 3)
            performancePlot(performance, clz.getPullTime, clz.getForwardTime, clz.getBackwardTime,
              clz.getPushTime, clz.getUpdateTime)
          case multi: MultiClassificationSummary =>
            f.height = 400
            f.width = 1200
            val loss = f.subplot(1, 3, 0)
            lossPlot(loss, clz.getHistLoss)

            val confusion = f.subplot(1, 3, 1)
            confusionPlot(confusion, multi.confusionMatrix, acc = multi.accuracy)

            val performance = f.subplot(1, 3, 2)
            performancePlot(performance, clz.getPullTime, clz.getForwardTime, clz.getBackwardTime,
              clz.getPushTime, clz.getUpdateTime)
        }
      case reg: RegressionTrainingStat =>
        f.height = 400
        f.width = 1200
        val loss = f.subplot(1, 3, 0)
        lossPlot(loss, reg.getHistLoss)

        val r2 = f.subplot(1, 3, 1)
        val summary = reg.getSummary
        val xValue = sample(100, summary.meanLabel, summary.stdLabel)
        r2Plot(r2, summary.r2, xValue, summary.slope, summary.intercept, summary.rmse)

        val performance = f.subplot(1, 3, 2)
        performancePlot(performance, reg.getPullTime, reg.getForwardTime, reg.getBackwardTime,
          reg.getPushTime, reg.getUpdateTime)
      case clu: ClusteringTrainingStat =>
    }

    f.saveas(fileName, dpi = 300)
  }

  def plotSummary(stat: ClassificationSummary, fileName: String): Unit = {
    val f = Figure()

    stat match {
      case bin: BinaryClassificationSummary =>
        f.width = 1000
        f.height = 500
        val roc = f.subplot(1, 2, 0)
        rocPlot(roc, bin.roc, auc = bin.areaUnderROC)

        val pr = f.subplot(1, 2, 1)
        prPlot(pr, bin.pr, auc = bin.areaUnderPR)
      case multi: MultiClassificationSummary =>
        f.width = 500
        f.height = 500

        val confusion = f.subplot(0)
        confusionPlot(confusion, multi.confusionMatrix, acc = multi.accuracy)
    }

    f.saveas(fileName, dpi = 300)
  }

  def plotSummary(stat: RegressionSummary, fileName: String): Unit = {
    val f = Figure()

    f.height = 500
    f.width = 500

    val r2 = f.subplot(0)
    val xValue = sample(100, stat.meanLabel, stat.stdLabel)
    r2Plot(r2, stat.r2, xValue, stat.slope, stat.intercept, stat.rmse)

    f.saveas(fileName, dpi = 300)
  }

  def plotSummary(stat: ClusteringSummary, fileName: String): Unit = ???

  private def sample(n: Int, mean: Double, std: Double): Array[Double] = {
    val rand = new Random
    (0 until n).toArray.map { _ => rand.nextGaussian() * std + mean }
  }
}
