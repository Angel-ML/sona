package com.tencent.angel.sona


import breeze.linalg._
import breeze.plot._

class VIZTEST extends TestBase {
  test("plot") {
    val f = Figure()
    val p1 = f.subplot(2, 1, 0)
    val x = linspace(0.0, 1.0)
    p1 += plot(x, x ^:^ 2.0)
    p1.xlabel = "x axis"
    p1.ylabel = "y axis"


    val p2 = f.subplot(2, 1, 1)
    val g = breeze.stats.distributions.Gaussian(0, 1)
    p2 += hist(g.sample(100000), 100)
    p2.title = "A normal distribution"


    f.saveas("lines.png")
  }

  test("scatter") {
    val f = Figure()
    val p1 = f.subplot(0)
    val x = DenseVector[Double](Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val y = DenseVector[Double](Array(0.2, 0.4, 0.05, 0.05, 0.2, 0.1))
    p1 += scatter(x, y, (i: Int) => y(i))
    p1.xlabel = "x axis"
    p1.ylabel = "y axis"


    f.saveas("scatter.png")
  }

  test("image") {
    val f2 = Figure()
    f2.width = 500
    f2.height = 500

    val labelFunc = new PartialFunction[(Int, Int), String] {
      override def isDefinedAt(x: (Int, Int)): Boolean = {
        x._1 >= 0 && x._2 >= 0
      }

      override def apply(v1: (Int, Int)): String = {
        s"(${v1._1}, ${v1._2}): 10"
      }
    }

    println(labelFunc(1, 4))
    val p = f2.subplot(0)
    p += image(DenseMatrix.rand(5, 5),
      name = "Confusion Matrix", labels = labelFunc)

    // Thread.sleep(100000)
    f2.saveas("image.png")
  }
}
