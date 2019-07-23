package com.tencent.client.master.protos;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: client_master.proto")
public final class AngelCleintMasterGrpc {

  private AngelCleintMasterGrpc() {}

  public static final String SERVICE_NAME = "ClientMaster.AngelCleintMaster";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterWorkerReq,
      com.tencent.client.master.protos.RegisterWorkerResp> getRegisterWorkerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterWorker",
      requestType = com.tencent.client.master.protos.RegisterWorkerReq.class,
      responseType = com.tencent.client.master.protos.RegisterWorkerResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterWorkerReq,
      com.tencent.client.master.protos.RegisterWorkerResp> getRegisterWorkerMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterWorkerReq, com.tencent.client.master.protos.RegisterWorkerResp> getRegisterWorkerMethod;
    if ((getRegisterWorkerMethod = AngelCleintMasterGrpc.getRegisterWorkerMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getRegisterWorkerMethod = AngelCleintMasterGrpc.getRegisterWorkerMethod) == null) {
          AngelCleintMasterGrpc.getRegisterWorkerMethod = getRegisterWorkerMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.RegisterWorkerReq, com.tencent.client.master.protos.RegisterWorkerResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "RegisterWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterWorkerReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterWorkerResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("RegisterWorker"))
                  .build();
          }
        }
     }
     return getRegisterWorkerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterTaskReq,
      com.tencent.client.master.protos.RegisterTaskResp> getRegisterTaskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterTask",
      requestType = com.tencent.client.master.protos.RegisterTaskReq.class,
      responseType = com.tencent.client.master.protos.RegisterTaskResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterTaskReq,
      com.tencent.client.master.protos.RegisterTaskResp> getRegisterTaskMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterTaskReq, com.tencent.client.master.protos.RegisterTaskResp> getRegisterTaskMethod;
    if ((getRegisterTaskMethod = AngelCleintMasterGrpc.getRegisterTaskMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getRegisterTaskMethod = AngelCleintMasterGrpc.getRegisterTaskMethod) == null) {
          AngelCleintMasterGrpc.getRegisterTaskMethod = getRegisterTaskMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.RegisterTaskReq, com.tencent.client.master.protos.RegisterTaskResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "RegisterTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterTaskReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterTaskResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("RegisterTask"))
                  .build();
          }
        }
     }
     return getRegisterTaskMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq,
      com.tencent.client.common.protos.VoidResp> getSetAngelLocationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetAngelLocation",
      requestType = com.tencent.client.master.protos.SetAngelLocationReq.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq,
      com.tencent.client.common.protos.VoidResp> getSetAngelLocationMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq, com.tencent.client.common.protos.VoidResp> getSetAngelLocationMethod;
    if ((getSetAngelLocationMethod = AngelCleintMasterGrpc.getSetAngelLocationMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getSetAngelLocationMethod = AngelCleintMasterGrpc.getSetAngelLocationMethod) == null) {
          AngelCleintMasterGrpc.getSetAngelLocationMethod = getSetAngelLocationMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.SetAngelLocationReq, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "SetAngelLocation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.SetAngelLocationReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("SetAngelLocation"))
                  .build();
          }
        }
     }
     return getSetAngelLocationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq,
      com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAngelLocation",
      requestType = com.tencent.client.common.protos.VoidReq.class,
      responseType = com.tencent.client.master.protos.GetAngelLocationResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq,
      com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq, com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod;
    if ((getGetAngelLocationMethod = AngelCleintMasterGrpc.getGetAngelLocationMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getGetAngelLocationMethod = AngelCleintMasterGrpc.getGetAngelLocationMethod) == null) {
          AngelCleintMasterGrpc.getGetAngelLocationMethod = getGetAngelLocationMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.common.protos.VoidReq, com.tencent.client.master.protos.GetAngelLocationResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "GetAngelLocation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.GetAngelLocationResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("GetAngelLocation"))
                  .build();
          }
        }
     }
     return getGetAngelLocationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.HeartBeatReq,
      com.tencent.client.master.protos.HeartBeatResp> getHeartBeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HeartBeat",
      requestType = com.tencent.client.master.protos.HeartBeatReq.class,
      responseType = com.tencent.client.master.protos.HeartBeatResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.HeartBeatReq,
      com.tencent.client.master.protos.HeartBeatResp> getHeartBeatMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.HeartBeatReq, com.tencent.client.master.protos.HeartBeatResp> getHeartBeatMethod;
    if ((getHeartBeatMethod = AngelCleintMasterGrpc.getHeartBeatMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getHeartBeatMethod = AngelCleintMasterGrpc.getHeartBeatMethod) == null) {
          AngelCleintMasterGrpc.getHeartBeatMethod = getHeartBeatMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.HeartBeatReq, com.tencent.client.master.protos.HeartBeatResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "HeartBeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.HeartBeatReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.HeartBeatResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("HeartBeat"))
                  .build();
          }
        }
     }
     return getHeartBeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.ClockReq,
      com.tencent.client.master.protos.ClockResp> getClockMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Clock",
      requestType = com.tencent.client.master.protos.ClockReq.class,
      responseType = com.tencent.client.master.protos.ClockResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.ClockReq,
      com.tencent.client.master.protos.ClockResp> getClockMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.ClockReq, com.tencent.client.master.protos.ClockResp> getClockMethod;
    if ((getClockMethod = AngelCleintMasterGrpc.getClockMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getClockMethod = AngelCleintMasterGrpc.getClockMethod) == null) {
          AngelCleintMasterGrpc.getClockMethod = getClockMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.ClockReq, com.tencent.client.master.protos.ClockResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "Clock"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.ClockReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.ClockResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("Clock"))
                  .build();
          }
        }
     }
     return getClockMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetClockMapReq,
      com.tencent.client.master.protos.GetClockMapResp> getGetClockMapMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetClockMap",
      requestType = com.tencent.client.master.protos.GetClockMapReq.class,
      responseType = com.tencent.client.master.protos.GetClockMapResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetClockMapReq,
      com.tencent.client.master.protos.GetClockMapResp> getGetClockMapMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetClockMapReq, com.tencent.client.master.protos.GetClockMapResp> getGetClockMapMethod;
    if ((getGetClockMapMethod = AngelCleintMasterGrpc.getGetClockMapMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getGetClockMapMethod = AngelCleintMasterGrpc.getGetClockMapMethod) == null) {
          AngelCleintMasterGrpc.getGetClockMapMethod = getGetClockMapMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.GetClockMapReq, com.tencent.client.master.protos.GetClockMapResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "GetClockMap"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.GetClockMapReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.GetClockMapResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("GetClockMap"))
                  .build();
          }
        }
     }
     return getGetClockMapMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq,
      com.tencent.client.master.protos.GetGlobalBatchResp> getGetGlobalBatchSizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGlobalBatchSize",
      requestType = com.tencent.client.common.protos.VoidReq.class,
      responseType = com.tencent.client.master.protos.GetGlobalBatchResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq,
      com.tencent.client.master.protos.GetGlobalBatchResp> getGetGlobalBatchSizeMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.common.protos.VoidReq, com.tencent.client.master.protos.GetGlobalBatchResp> getGetGlobalBatchSizeMethod;
    if ((getGetGlobalBatchSizeMethod = AngelCleintMasterGrpc.getGetGlobalBatchSizeMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getGetGlobalBatchSizeMethod = AngelCleintMasterGrpc.getGetGlobalBatchSizeMethod) == null) {
          AngelCleintMasterGrpc.getGetGlobalBatchSizeMethod = getGetGlobalBatchSizeMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.common.protos.VoidReq, com.tencent.client.master.protos.GetGlobalBatchResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "GetGlobalBatchSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.GetGlobalBatchResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("GetGlobalBatchSize"))
                  .build();
          }
        }
     }
     return getGetGlobalBatchSizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.common.protos.CompleteTaskReq,
      com.tencent.client.common.protos.VoidResp> getCompleteTaskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompleteTask",
      requestType = com.tencent.client.common.protos.CompleteTaskReq.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.common.protos.CompleteTaskReq,
      com.tencent.client.common.protos.VoidResp> getCompleteTaskMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.common.protos.CompleteTaskReq, com.tencent.client.common.protos.VoidResp> getCompleteTaskMethod;
    if ((getCompleteTaskMethod = AngelCleintMasterGrpc.getCompleteTaskMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getCompleteTaskMethod = AngelCleintMasterGrpc.getCompleteTaskMethod) == null) {
          AngelCleintMasterGrpc.getCompleteTaskMethod = getCompleteTaskMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.common.protos.CompleteTaskReq, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "CompleteTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.CompleteTaskReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("CompleteTask"))
                  .build();
          }
        }
     }
     return getCompleteTaskMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AngelCleintMasterStub newStub(io.grpc.Channel channel) {
    return new AngelCleintMasterStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AngelCleintMasterBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new AngelCleintMasterBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AngelCleintMasterFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new AngelCleintMasterFutureStub(channel);
  }

  /**
   */
  public static abstract class AngelCleintMasterImplBase implements io.grpc.BindableService {

    /**
     */
    public void registerWorker(com.tencent.client.master.protos.RegisterWorkerReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterWorkerResp> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterWorkerMethod(), responseObserver);
    }

    /**
     */
    public void registerTask(com.tencent.client.master.protos.RegisterTaskReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterTaskResp> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterTaskMethod(), responseObserver);
    }

    /**
     */
    public void setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getSetAngelLocationMethod(), responseObserver);
    }

    /**
     */
    public void getAngelLocation(com.tencent.client.common.protos.VoidReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetAngelLocationResp> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAngelLocationMethod(), responseObserver);
    }

    /**
     */
    public void heartBeat(com.tencent.client.master.protos.HeartBeatReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.HeartBeatResp> responseObserver) {
      asyncUnimplementedUnaryCall(getHeartBeatMethod(), responseObserver);
    }

    /**
     */
    public void clock(com.tencent.client.master.protos.ClockReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.ClockResp> responseObserver) {
      asyncUnimplementedUnaryCall(getClockMethod(), responseObserver);
    }

    /**
     */
    public void getClockMap(com.tencent.client.master.protos.GetClockMapReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetClockMapResp> responseObserver) {
      asyncUnimplementedUnaryCall(getGetClockMapMethod(), responseObserver);
    }

    /**
     */
    public void getGlobalBatchSize(com.tencent.client.common.protos.VoidReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetGlobalBatchResp> responseObserver) {
      asyncUnimplementedUnaryCall(getGetGlobalBatchSizeMethod(), responseObserver);
    }

    /**
     */
    public void completeTask(com.tencent.client.common.protos.CompleteTaskReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getCompleteTaskMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRegisterWorkerMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.RegisterWorkerReq,
                com.tencent.client.master.protos.RegisterWorkerResp>(
                  this, METHODID_REGISTER_WORKER)))
          .addMethod(
            getRegisterTaskMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.RegisterTaskReq,
                com.tencent.client.master.protos.RegisterTaskResp>(
                  this, METHODID_REGISTER_TASK)))
          .addMethod(
            getSetAngelLocationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.SetAngelLocationReq,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_SET_ANGEL_LOCATION)))
          .addMethod(
            getGetAngelLocationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.common.protos.VoidReq,
                com.tencent.client.master.protos.GetAngelLocationResp>(
                  this, METHODID_GET_ANGEL_LOCATION)))
          .addMethod(
            getHeartBeatMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.HeartBeatReq,
                com.tencent.client.master.protos.HeartBeatResp>(
                  this, METHODID_HEART_BEAT)))
          .addMethod(
            getClockMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.ClockReq,
                com.tencent.client.master.protos.ClockResp>(
                  this, METHODID_CLOCK)))
          .addMethod(
            getGetClockMapMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.GetClockMapReq,
                com.tencent.client.master.protos.GetClockMapResp>(
                  this, METHODID_GET_CLOCK_MAP)))
          .addMethod(
            getGetGlobalBatchSizeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.common.protos.VoidReq,
                com.tencent.client.master.protos.GetGlobalBatchResp>(
                  this, METHODID_GET_GLOBAL_BATCH_SIZE)))
          .addMethod(
            getCompleteTaskMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.common.protos.CompleteTaskReq,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_COMPLETE_TASK)))
          .build();
    }
  }

  /**
   */
  public static final class AngelCleintMasterStub extends io.grpc.stub.AbstractStub<AngelCleintMasterStub> {
    private AngelCleintMasterStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AngelCleintMasterStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AngelCleintMasterStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AngelCleintMasterStub(channel, callOptions);
    }

    /**
     */
    public void registerWorker(com.tencent.client.master.protos.RegisterWorkerReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterWorkerResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterWorkerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void registerTask(com.tencent.client.master.protos.RegisterTaskReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterTaskResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterTaskMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetAngelLocationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAngelLocation(com.tencent.client.common.protos.VoidReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetAngelLocationResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAngelLocationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartBeat(com.tencent.client.master.protos.HeartBeatReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.HeartBeatResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHeartBeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void clock(com.tencent.client.master.protos.ClockReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.ClockResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getClockMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getClockMap(com.tencent.client.master.protos.GetClockMapReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetClockMapResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetClockMapMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getGlobalBatchSize(com.tencent.client.common.protos.VoidReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetGlobalBatchResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetGlobalBatchSizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void completeTask(com.tencent.client.common.protos.CompleteTaskReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCompleteTaskMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AngelCleintMasterBlockingStub extends io.grpc.stub.AbstractStub<AngelCleintMasterBlockingStub> {
    private AngelCleintMasterBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AngelCleintMasterBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AngelCleintMasterBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AngelCleintMasterBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.tencent.client.master.protos.RegisterWorkerResp registerWorker(com.tencent.client.master.protos.RegisterWorkerReq request) {
      return blockingUnaryCall(
          getChannel(), getRegisterWorkerMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.RegisterTaskResp registerTask(com.tencent.client.master.protos.RegisterTaskReq request) {
      return blockingUnaryCall(
          getChannel(), getRegisterTaskMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request) {
      return blockingUnaryCall(
          getChannel(), getSetAngelLocationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.GetAngelLocationResp getAngelLocation(com.tencent.client.common.protos.VoidReq request) {
      return blockingUnaryCall(
          getChannel(), getGetAngelLocationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.HeartBeatResp heartBeat(com.tencent.client.master.protos.HeartBeatReq request) {
      return blockingUnaryCall(
          getChannel(), getHeartBeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.ClockResp clock(com.tencent.client.master.protos.ClockReq request) {
      return blockingUnaryCall(
          getChannel(), getClockMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.GetClockMapResp getClockMap(com.tencent.client.master.protos.GetClockMapReq request) {
      return blockingUnaryCall(
          getChannel(), getGetClockMapMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.GetGlobalBatchResp getGlobalBatchSize(com.tencent.client.common.protos.VoidReq request) {
      return blockingUnaryCall(
          getChannel(), getGetGlobalBatchSizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp completeTask(com.tencent.client.common.protos.CompleteTaskReq request) {
      return blockingUnaryCall(
          getChannel(), getCompleteTaskMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AngelCleintMasterFutureStub extends io.grpc.stub.AbstractStub<AngelCleintMasterFutureStub> {
    private AngelCleintMasterFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private AngelCleintMasterFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AngelCleintMasterFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new AngelCleintMasterFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.RegisterWorkerResp> registerWorker(
        com.tencent.client.master.protos.RegisterWorkerReq request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterWorkerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.RegisterTaskResp> registerTask(
        com.tencent.client.master.protos.RegisterTaskReq request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterTaskMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> setAngelLocation(
        com.tencent.client.master.protos.SetAngelLocationReq request) {
      return futureUnaryCall(
          getChannel().newCall(getSetAngelLocationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.GetAngelLocationResp> getAngelLocation(
        com.tencent.client.common.protos.VoidReq request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAngelLocationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.HeartBeatResp> heartBeat(
        com.tencent.client.master.protos.HeartBeatReq request) {
      return futureUnaryCall(
          getChannel().newCall(getHeartBeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.ClockResp> clock(
        com.tencent.client.master.protos.ClockReq request) {
      return futureUnaryCall(
          getChannel().newCall(getClockMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.GetClockMapResp> getClockMap(
        com.tencent.client.master.protos.GetClockMapReq request) {
      return futureUnaryCall(
          getChannel().newCall(getGetClockMapMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.GetGlobalBatchResp> getGlobalBatchSize(
        com.tencent.client.common.protos.VoidReq request) {
      return futureUnaryCall(
          getChannel().newCall(getGetGlobalBatchSizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> completeTask(
        com.tencent.client.common.protos.CompleteTaskReq request) {
      return futureUnaryCall(
          getChannel().newCall(getCompleteTaskMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_WORKER = 0;
  private static final int METHODID_REGISTER_TASK = 1;
  private static final int METHODID_SET_ANGEL_LOCATION = 2;
  private static final int METHODID_GET_ANGEL_LOCATION = 3;
  private static final int METHODID_HEART_BEAT = 4;
  private static final int METHODID_CLOCK = 5;
  private static final int METHODID_GET_CLOCK_MAP = 6;
  private static final int METHODID_GET_GLOBAL_BATCH_SIZE = 7;
  private static final int METHODID_COMPLETE_TASK = 8;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AngelCleintMasterImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AngelCleintMasterImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER_WORKER:
          serviceImpl.registerWorker((com.tencent.client.master.protos.RegisterWorkerReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterWorkerResp>) responseObserver);
          break;
        case METHODID_REGISTER_TASK:
          serviceImpl.registerTask((com.tencent.client.master.protos.RegisterTaskReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterTaskResp>) responseObserver);
          break;
        case METHODID_SET_ANGEL_LOCATION:
          serviceImpl.setAngelLocation((com.tencent.client.master.protos.SetAngelLocationReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_GET_ANGEL_LOCATION:
          serviceImpl.getAngelLocation((com.tencent.client.common.protos.VoidReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetAngelLocationResp>) responseObserver);
          break;
        case METHODID_HEART_BEAT:
          serviceImpl.heartBeat((com.tencent.client.master.protos.HeartBeatReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.HeartBeatResp>) responseObserver);
          break;
        case METHODID_CLOCK:
          serviceImpl.clock((com.tencent.client.master.protos.ClockReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.ClockResp>) responseObserver);
          break;
        case METHODID_GET_CLOCK_MAP:
          serviceImpl.getClockMap((com.tencent.client.master.protos.GetClockMapReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetClockMapResp>) responseObserver);
          break;
        case METHODID_GET_GLOBAL_BATCH_SIZE:
          serviceImpl.getGlobalBatchSize((com.tencent.client.common.protos.VoidReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetGlobalBatchResp>) responseObserver);
          break;
        case METHODID_COMPLETE_TASK:
          serviceImpl.completeTask((com.tencent.client.common.protos.CompleteTaskReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class AngelCleintMasterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AngelCleintMasterBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.tencent.client.master.protos.ClientMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AngelCleintMaster");
    }
  }

  private static final class AngelCleintMasterFileDescriptorSupplier
      extends AngelCleintMasterBaseDescriptorSupplier {
    AngelCleintMasterFileDescriptorSupplier() {}
  }

  private static final class AngelCleintMasterMethodDescriptorSupplier
      extends AngelCleintMasterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AngelCleintMasterMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AngelCleintMasterFileDescriptorSupplier())
              .addMethod(getRegisterWorkerMethod())
              .addMethod(getRegisterTaskMethod())
              .addMethod(getSetAngelLocationMethod())
              .addMethod(getGetAngelLocationMethod())
              .addMethod(getHeartBeatMethod())
              .addMethod(getClockMethod())
              .addMethod(getGetClockMapMethod())
              .addMethod(getGetGlobalBatchSizeMethod())
              .addMethod(getCompleteTaskMethod())
              .build();
        }
      }
    }
    return result;
  }
}
