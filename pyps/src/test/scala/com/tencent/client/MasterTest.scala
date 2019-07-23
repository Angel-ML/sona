package com.tencent.client


import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executor, Executors}

import com.tencent.angel.common.location.Location
import com.tencent.client.worker.{MStub, Task}
import org.scalatest.FunSuite

class MasterTest extends FunSuite {
  private val master = new MStub("localhost", 8980)
  private val pool = Executors.newCachedThreadPool()
  private val numTasks = new AtomicInteger(0)

  class CreateTask extends Runnable {
    override def run(): Unit = {
      val flag = numTasks.getAndIncrement()
      val task = new Task(master, flag == 0)
      task.register()

      println(s"task ${task.taskId} is registered")
      task.epoch = 1
      task.batch = 1
      task.batchSize = 100

      Thread.sleep((Math.random() * 5000).toInt)

      println(s"task ${task.taskId} begin to sync")
      task.sync()
      println(s"task ${task.taskId} finish to sync")
    }
  }

  test("registerWorker") {
    master.registerWorker()
    master.shutdown()
  }

  test("setAngelLocation") {
    master.registerWorker()
    master.setAngelLocation(new Location("localhost", 8981))
    val loc = master.getAngelLocation
    println(loc.toString)


    (0 until 3).foreach{ _ =>
      pool.execute(new CreateTask)
    }

    val timeout = 300000
    var acc = 0
    while (acc < timeout) {
      Thread.sleep(100)
      acc += 100
    }
    //master.shutdown()
  }

  test("getAngelLocation"){

  }

  test("registerTask"){

  }

  test("clock"){

  }

  test("getClockMap"){

  }

  test("completeTask"){

  }

  test("getGlobalBatchSize") {

  }

  test("sendHeartBeat") {

  }
}
