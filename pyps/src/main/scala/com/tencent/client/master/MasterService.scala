package com.tencent.client.master

import java.util
import java.util.logging.Logger

import com.tencent.angel.common.location.Location
import com.tencent.client.common.AsyncModel.AsyncModel
import io.grpc.stub.StreamObserver
import com.tencent.client.master.protos._

import scala.collection.mutable


class MasterService(val numTask: Int, val syncModel: AsyncModel, val conf: util.Map[String, String])
  extends AngelCleintMasterGrpc.AngelCleintMasterImplBase {
  private val logger = Logger.getLogger(classOf[MasterService].getSimpleName)

  private val workerMap = new mutable.HashMap[Long, Location]()

  override def register(request: RegisterReq, responseObserver: StreamObserver[RegisterResp]): Unit = synchronized {
    val location = new Location(request.getHost, request.getPort)
    val exists = workerMap.values.exists{ loc => loc.getIp == location.getIp && loc.getPort == location.getPort}

    val workerId = if (exists) {
      logger.info(s"${location.getIp}:${location.getPort} has registered! ")
      workerMap.collectFirst{
        case (wId: Long, loc: Location) if loc.getIp == location.getIp && loc.getPort == location.getPort => wId
      }.get
    } else {
      val wId = workerMap.size
      workerMap.put(wId, location)

      wId
    }



    responseObserver.onNext()
    responseObserver.onCompleted()
  }

  /**
    */
  def setAngelLocation(request: SetAngelLocationReq, responseObserver: StreamObserver[SetAngelLocationResp]): Unit = {
    responseObserver.onNext()
    responseObserver.onCompleted()
  }

  def getAngelLocation(request: GetAngelLocationReq, responseObserver: StreamObserver[GetAngelLocationResp]): Unit = {
    responseObserver.onNext()
    responseObserver.onCompleted()
  }

  def heartBeat(request: HeartBeatReq, responseObserver: StreamObserver[HeartBeatResp]): Unit = {
    responseObserver.onNext()
    responseObserver.onCompleted()
  }

  def sync(request: SyncReq, responseObserver: StreamObserver[SyncResp]): Unit = {
    responseObserver.onNext()
    responseObserver.onCompleted()
  }
}
