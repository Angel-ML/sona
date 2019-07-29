package com.tencent.client.worker

case class TaskInfo(taskId: Long, numTask: Int, clock: Map[Long, Int])
