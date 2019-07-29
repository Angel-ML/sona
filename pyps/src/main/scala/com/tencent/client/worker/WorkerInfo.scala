package com.tencent.client.worker

import com.tencent.client.common.protos.AsyncModel
import org.apache.hadoop.conf.Configuration

case class WorkerInfo(workId: Long, isChief: Boolean, asyncModel: AsyncModel, heartBeatInterval: Int, conf: Configuration)