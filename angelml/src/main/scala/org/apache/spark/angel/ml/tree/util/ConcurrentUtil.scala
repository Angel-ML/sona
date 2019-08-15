package org.apache.spark.angel.ml.tree.util

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

object ConcurrentUtil {
  private[tree] var numThread: Int = 1
  private[tree] var threadPool: ExecutorService = _
  private[tree] val DEFAULT_BATCH_SIZE = 1000000

  private[tree] def reset(parallelism: Int): Unit = {
    ConcurrentUtil.getClass.synchronized {
      this.numThread = parallelism
      this.threadPool = Executors.newFixedThreadPool(parallelism)
    }
  }

  private[tree] def rangeParallel[A](f: (Int, Int) => A, start: Int, end: Int,
                                    batchSize: Int = DEFAULT_BATCH_SIZE): Array[Future[A]] = {
    val futures = Array.ofDim[Future[A]](MathUtil.idivCeil(end - start, batchSize))
    var cur = start
    var threadId = 0
    while (cur < end) {
      val i = cur
      val j = (cur + batchSize) min end
      futures(threadId) = threadPool.submit(new Callable[A] {
        override def call(): A = f(i, j)
      })
      cur = j
      threadId += 1
    }
    futures
  }

  private[tree] def shutdown(): Unit = ConcurrentUtil.getClass.synchronized {
    if (threadPool != null)
      threadPool.shutdown()
  }

}
