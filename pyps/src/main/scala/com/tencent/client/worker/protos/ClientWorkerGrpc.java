package com.tencent.client.worker.protos;

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
    comments = "Source: client_worker.proto")
public final class ClientWorkerGrpc {

  private ClientWorkerGrpc() {}

  public static final String SERVICE_NAME = "ClientMaster.ClientWorker";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCTensor,
      com.tencent.client.worker.protos.CreateResp> getCreateTensorMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTensor",
      requestType = com.tencent.client.worker.protos.RPCTensor.class,
      responseType = com.tencent.client.worker.protos.CreateResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCTensor,
      com.tencent.client.worker.protos.CreateResp> getCreateTensorMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCTensor, com.tencent.client.worker.protos.CreateResp> getCreateTensorMethod;
    if ((getCreateTensorMethod = ClientWorkerGrpc.getCreateTensorMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getCreateTensorMethod = ClientWorkerGrpc.getCreateTensorMethod) == null) {
          ClientWorkerGrpc.getCreateTensorMethod = getCreateTensorMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.RPCTensor, com.tencent.client.worker.protos.CreateResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "CreateTensor"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.RPCTensor.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.CreateResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("CreateTensor"))
                  .build();
          }
        }
     }
     return getCreateTensorMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCVariable,
      com.tencent.client.worker.protos.CreateResp> getCreateVariableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateVariable",
      requestType = com.tencent.client.worker.protos.RPCVariable.class,
      responseType = com.tencent.client.worker.protos.CreateResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCVariable,
      com.tencent.client.worker.protos.CreateResp> getCreateVariableMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCVariable, com.tencent.client.worker.protos.CreateResp> getCreateVariableMethod;
    if ((getCreateVariableMethod = ClientWorkerGrpc.getCreateVariableMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getCreateVariableMethod = ClientWorkerGrpc.getCreateVariableMethod) == null) {
          ClientWorkerGrpc.getCreateVariableMethod = getCreateVariableMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.RPCVariable, com.tencent.client.worker.protos.CreateResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "CreateVariable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.RPCVariable.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.CreateResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("CreateVariable"))
                  .build();
          }
        }
     }
     return getCreateVariableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCEmbedding,
      com.tencent.client.worker.protos.CreateResp> getCreateEmbeddingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateEmbedding",
      requestType = com.tencent.client.worker.protos.RPCEmbedding.class,
      responseType = com.tencent.client.worker.protos.CreateResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCEmbedding,
      com.tencent.client.worker.protos.CreateResp> getCreateEmbeddingMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.RPCEmbedding, com.tencent.client.worker.protos.CreateResp> getCreateEmbeddingMethod;
    if ((getCreateEmbeddingMethod = ClientWorkerGrpc.getCreateEmbeddingMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getCreateEmbeddingMethod = ClientWorkerGrpc.getCreateEmbeddingMethod) == null) {
          ClientWorkerGrpc.getCreateEmbeddingMethod = getCreateEmbeddingMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.RPCEmbedding, com.tencent.client.worker.protos.CreateResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "CreateEmbedding"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.RPCEmbedding.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.CreateResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("CreateEmbedding"))
                  .build();
          }
        }
     }
     return getCreateEmbeddingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getInitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Init",
      requestType = com.tencent.client.worker.protos.TensorLike.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getInitMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp> getInitMethod;
    if ((getInitMethod = ClientWorkerGrpc.getInitMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getInitMethod = ClientWorkerGrpc.getInitMethod) == null) {
          ClientWorkerGrpc.getInitMethod = getInitMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Init"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.TensorLike.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Init"))
                  .build();
          }
        }
     }
     return getInitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.LoadTensorLike,
      com.tencent.client.common.protos.VoidResp> getLoadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Load",
      requestType = com.tencent.client.worker.protos.LoadTensorLike.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.LoadTensorLike,
      com.tencent.client.common.protos.VoidResp> getLoadMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.LoadTensorLike, com.tencent.client.common.protos.VoidResp> getLoadMethod;
    if ((getLoadMethod = ClientWorkerGrpc.getLoadMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getLoadMethod = ClientWorkerGrpc.getLoadMethod) == null) {
          ClientWorkerGrpc.getLoadMethod = getLoadMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.LoadTensorLike, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Load"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.LoadTensorLike.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Load"))
                  .build();
          }
        }
     }
     return getLoadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SaveTensorLike,
      com.tencent.client.common.protos.VoidResp> getSaveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Save",
      requestType = com.tencent.client.worker.protos.SaveTensorLike.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SaveTensorLike,
      com.tencent.client.common.protos.VoidResp> getSaveMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SaveTensorLike, com.tencent.client.common.protos.VoidResp> getSaveMethod;
    if ((getSaveMethod = ClientWorkerGrpc.getSaveMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getSaveMethod = ClientWorkerGrpc.getSaveMethod) == null) {
          ClientWorkerGrpc.getSaveMethod = getSaveMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.SaveTensorLike, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Save"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.SaveTensorLike.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Save"))
                  .build();
          }
        }
     }
     return getSaveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PullRequest,
      com.tencent.client.worker.protos.PullResponse> getPullMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Pull",
      requestType = com.tencent.client.worker.protos.PullRequest.class,
      responseType = com.tencent.client.worker.protos.PullResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PullRequest,
      com.tencent.client.worker.protos.PullResponse> getPullMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PullRequest, com.tencent.client.worker.protos.PullResponse> getPullMethod;
    if ((getPullMethod = ClientWorkerGrpc.getPullMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getPullMethod = ClientWorkerGrpc.getPullMethod) == null) {
          ClientWorkerGrpc.getPullMethod = getPullMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.PullRequest, com.tencent.client.worker.protos.PullResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Pull"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.PullRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.PullResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Pull"))
                  .build();
          }
        }
     }
     return getPullMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PushRequest,
      com.tencent.client.common.protos.VoidResp> getPushMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Push",
      requestType = com.tencent.client.worker.protos.PushRequest.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PushRequest,
      com.tencent.client.common.protos.VoidResp> getPushMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.PushRequest, com.tencent.client.common.protos.VoidResp> getPushMethod;
    if ((getPushMethod = ClientWorkerGrpc.getPushMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getPushMethod = ClientWorkerGrpc.getPushMethod) == null) {
          ClientWorkerGrpc.getPushMethod = getPushMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.PushRequest, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Push"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.PushRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Push"))
                  .build();
          }
        }
     }
     return getPushMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getReleaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Release",
      requestType = com.tencent.client.worker.protos.TensorLike.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getReleaseMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp> getReleaseMethod;
    if ((getReleaseMethod = ClientWorkerGrpc.getReleaseMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getReleaseMethod = ClientWorkerGrpc.getReleaseMethod) == null) {
          ClientWorkerGrpc.getReleaseMethod = getReleaseMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Release"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.TensorLike.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Release"))
                  .build();
          }
        }
     }
     return getReleaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getUpdateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Update",
      requestType = com.tencent.client.worker.protos.TensorLike.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike,
      com.tencent.client.common.protos.VoidResp> getUpdateMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp> getUpdateMethod;
    if ((getUpdateMethod = ClientWorkerGrpc.getUpdateMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getUpdateMethod = ClientWorkerGrpc.getUpdateMethod) == null) {
          ClientWorkerGrpc.getUpdateMethod = getUpdateMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.TensorLike, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Update"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.TensorLike.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Update"))
                  .build();
          }
        }
     }
     return getUpdateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SyncRequest,
      com.tencent.client.common.protos.VoidResp> getSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Sync",
      requestType = com.tencent.client.worker.protos.SyncRequest.class,
      responseType = com.tencent.client.common.protos.VoidResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SyncRequest,
      com.tencent.client.common.protos.VoidResp> getSyncMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.worker.protos.SyncRequest, com.tencent.client.common.protos.VoidResp> getSyncMethod;
    if ((getSyncMethod = ClientWorkerGrpc.getSyncMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getSyncMethod = ClientWorkerGrpc.getSyncMethod) == null) {
          ClientWorkerGrpc.getSyncMethod = getSyncMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.worker.protos.SyncRequest, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "Sync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.worker.protos.SyncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("Sync"))
                  .build();
          }
        }
     }
     return getSyncMethod;
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
    if ((getCompleteTaskMethod = ClientWorkerGrpc.getCompleteTaskMethod) == null) {
      synchronized (ClientWorkerGrpc.class) {
        if ((getCompleteTaskMethod = ClientWorkerGrpc.getCompleteTaskMethod) == null) {
          ClientWorkerGrpc.getCompleteTaskMethod = getCompleteTaskMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.common.protos.CompleteTaskReq, com.tencent.client.common.protos.VoidResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.ClientWorker", "CompleteTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.CompleteTaskReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.common.protos.VoidResp.getDefaultInstance()))
                  .setSchemaDescriptor(new ClientWorkerMethodDescriptorSupplier("CompleteTask"))
                  .build();
          }
        }
     }
     return getCompleteTaskMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ClientWorkerStub newStub(io.grpc.Channel channel) {
    return new ClientWorkerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ClientWorkerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ClientWorkerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ClientWorkerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ClientWorkerFutureStub(channel);
  }

  /**
   */
  public static abstract class ClientWorkerImplBase implements io.grpc.BindableService {

    /**
     */
    public void createTensor(com.tencent.client.worker.protos.RPCTensor request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateTensorMethod(), responseObserver);
    }

    /**
     */
    public void createVariable(com.tencent.client.worker.protos.RPCVariable request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateVariableMethod(), responseObserver);
    }

    /**
     */
    public void createEmbedding(com.tencent.client.worker.protos.RPCEmbedding request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateEmbeddingMethod(), responseObserver);
    }

    /**
     */
    public void init(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getInitMethod(), responseObserver);
    }

    /**
     */
    public void load(com.tencent.client.worker.protos.LoadTensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getLoadMethod(), responseObserver);
    }

    /**
     */
    public void save(com.tencent.client.worker.protos.SaveTensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getSaveMethod(), responseObserver);
    }

    /**
     */
    public void pull(com.tencent.client.worker.protos.PullRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.PullResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPullMethod(), responseObserver);
    }

    /**
     */
    public void push(com.tencent.client.worker.protos.PushRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getPushMethod(), responseObserver);
    }

    /**
     */
    public void release(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getReleaseMethod(), responseObserver);
    }

    /**
     */
    public void update(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getUpdateMethod(), responseObserver);
    }

    /**
     */
    public void sync(com.tencent.client.worker.protos.SyncRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
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
            getCreateTensorMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.RPCTensor,
                com.tencent.client.worker.protos.CreateResp>(
                  this, METHODID_CREATE_TENSOR)))
          .addMethod(
            getCreateVariableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.RPCVariable,
                com.tencent.client.worker.protos.CreateResp>(
                  this, METHODID_CREATE_VARIABLE)))
          .addMethod(
            getCreateEmbeddingMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.RPCEmbedding,
                com.tencent.client.worker.protos.CreateResp>(
                  this, METHODID_CREATE_EMBEDDING)))
          .addMethod(
            getInitMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.TensorLike,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_INIT)))
          .addMethod(
            getLoadMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.LoadTensorLike,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_LOAD)))
          .addMethod(
            getSaveMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.SaveTensorLike,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_SAVE)))
          .addMethod(
            getPullMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.PullRequest,
                com.tencent.client.worker.protos.PullResponse>(
                  this, METHODID_PULL)))
          .addMethod(
            getPushMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.PushRequest,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_PUSH)))
          .addMethod(
            getReleaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.TensorLike,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_RELEASE)))
          .addMethod(
            getUpdateMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.TensorLike,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_UPDATE)))
          .addMethod(
            getSyncMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.worker.protos.SyncRequest,
                com.tencent.client.common.protos.VoidResp>(
                  this, METHODID_SYNC)))
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
  public static final class ClientWorkerStub extends io.grpc.stub.AbstractStub<ClientWorkerStub> {
    private ClientWorkerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientWorkerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ClientWorkerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientWorkerStub(channel, callOptions);
    }

    /**
     */
    public void createTensor(com.tencent.client.worker.protos.RPCTensor request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateTensorMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createVariable(com.tencent.client.worker.protos.RPCVariable request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateVariableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createEmbedding(com.tencent.client.worker.protos.RPCEmbedding request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateEmbeddingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void init(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getInitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void load(com.tencent.client.worker.protos.LoadTensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getLoadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void save(com.tencent.client.worker.protos.SaveTensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSaveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pull(com.tencent.client.worker.protos.PullRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.PullResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPullMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void push(com.tencent.client.worker.protos.PushRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPushMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void release(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void update(com.tencent.client.worker.protos.TensorLike request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sync(com.tencent.client.worker.protos.SyncRequest request,
        io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
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
  public static final class ClientWorkerBlockingStub extends io.grpc.stub.AbstractStub<ClientWorkerBlockingStub> {
    private ClientWorkerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientWorkerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ClientWorkerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientWorkerBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.tencent.client.worker.protos.CreateResp createTensor(com.tencent.client.worker.protos.RPCTensor request) {
      return blockingUnaryCall(
          getChannel(), getCreateTensorMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.worker.protos.CreateResp createVariable(com.tencent.client.worker.protos.RPCVariable request) {
      return blockingUnaryCall(
          getChannel(), getCreateVariableMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.worker.protos.CreateResp createEmbedding(com.tencent.client.worker.protos.RPCEmbedding request) {
      return blockingUnaryCall(
          getChannel(), getCreateEmbeddingMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp init(com.tencent.client.worker.protos.TensorLike request) {
      return blockingUnaryCall(
          getChannel(), getInitMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp load(com.tencent.client.worker.protos.LoadTensorLike request) {
      return blockingUnaryCall(
          getChannel(), getLoadMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp save(com.tencent.client.worker.protos.SaveTensorLike request) {
      return blockingUnaryCall(
          getChannel(), getSaveMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.worker.protos.PullResponse pull(com.tencent.client.worker.protos.PullRequest request) {
      return blockingUnaryCall(
          getChannel(), getPullMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp push(com.tencent.client.worker.protos.PushRequest request) {
      return blockingUnaryCall(
          getChannel(), getPushMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp release(com.tencent.client.worker.protos.TensorLike request) {
      return blockingUnaryCall(
          getChannel(), getReleaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp update(com.tencent.client.worker.protos.TensorLike request) {
      return blockingUnaryCall(
          getChannel(), getUpdateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.common.protos.VoidResp sync(com.tencent.client.worker.protos.SyncRequest request) {
      return blockingUnaryCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
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
  public static final class ClientWorkerFutureStub extends io.grpc.stub.AbstractStub<ClientWorkerFutureStub> {
    private ClientWorkerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ClientWorkerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ClientWorkerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ClientWorkerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.worker.protos.CreateResp> createTensor(
        com.tencent.client.worker.protos.RPCTensor request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateTensorMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.worker.protos.CreateResp> createVariable(
        com.tencent.client.worker.protos.RPCVariable request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateVariableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.worker.protos.CreateResp> createEmbedding(
        com.tencent.client.worker.protos.RPCEmbedding request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateEmbeddingMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> init(
        com.tencent.client.worker.protos.TensorLike request) {
      return futureUnaryCall(
          getChannel().newCall(getInitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> load(
        com.tencent.client.worker.protos.LoadTensorLike request) {
      return futureUnaryCall(
          getChannel().newCall(getLoadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> save(
        com.tencent.client.worker.protos.SaveTensorLike request) {
      return futureUnaryCall(
          getChannel().newCall(getSaveMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.worker.protos.PullResponse> pull(
        com.tencent.client.worker.protos.PullRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPullMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> push(
        com.tencent.client.worker.protos.PushRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPushMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> release(
        com.tencent.client.worker.protos.TensorLike request) {
      return futureUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> update(
        com.tencent.client.worker.protos.TensorLike request) {
      return futureUnaryCall(
          getChannel().newCall(getUpdateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> sync(
        com.tencent.client.worker.protos.SyncRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.common.protos.VoidResp> completeTask(
        com.tencent.client.common.protos.CompleteTaskReq request) {
      return futureUnaryCall(
          getChannel().newCall(getCompleteTaskMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_TENSOR = 0;
  private static final int METHODID_CREATE_VARIABLE = 1;
  private static final int METHODID_CREATE_EMBEDDING = 2;
  private static final int METHODID_INIT = 3;
  private static final int METHODID_LOAD = 4;
  private static final int METHODID_SAVE = 5;
  private static final int METHODID_PULL = 6;
  private static final int METHODID_PUSH = 7;
  private static final int METHODID_RELEASE = 8;
  private static final int METHODID_UPDATE = 9;
  private static final int METHODID_SYNC = 10;
  private static final int METHODID_COMPLETE_TASK = 11;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ClientWorkerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ClientWorkerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_TENSOR:
          serviceImpl.createTensor((com.tencent.client.worker.protos.RPCTensor) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp>) responseObserver);
          break;
        case METHODID_CREATE_VARIABLE:
          serviceImpl.createVariable((com.tencent.client.worker.protos.RPCVariable) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp>) responseObserver);
          break;
        case METHODID_CREATE_EMBEDDING:
          serviceImpl.createEmbedding((com.tencent.client.worker.protos.RPCEmbedding) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.CreateResp>) responseObserver);
          break;
        case METHODID_INIT:
          serviceImpl.init((com.tencent.client.worker.protos.TensorLike) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_LOAD:
          serviceImpl.load((com.tencent.client.worker.protos.LoadTensorLike) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_SAVE:
          serviceImpl.save((com.tencent.client.worker.protos.SaveTensorLike) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_PULL:
          serviceImpl.pull((com.tencent.client.worker.protos.PullRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.worker.protos.PullResponse>) responseObserver);
          break;
        case METHODID_PUSH:
          serviceImpl.push((com.tencent.client.worker.protos.PushRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_RELEASE:
          serviceImpl.release((com.tencent.client.worker.protos.TensorLike) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_UPDATE:
          serviceImpl.update((com.tencent.client.worker.protos.TensorLike) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
          break;
        case METHODID_SYNC:
          serviceImpl.sync((com.tencent.client.worker.protos.SyncRequest) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.common.protos.VoidResp>) responseObserver);
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

  private static abstract class ClientWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ClientWorkerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.tencent.client.worker.protos.ClientWorkerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ClientWorker");
    }
  }

  private static final class ClientWorkerFileDescriptorSupplier
      extends ClientWorkerBaseDescriptorSupplier {
    ClientWorkerFileDescriptorSupplier() {}
  }

  private static final class ClientWorkerMethodDescriptorSupplier
      extends ClientWorkerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ClientWorkerMethodDescriptorSupplier(String methodName) {
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
      synchronized (ClientWorkerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ClientWorkerFileDescriptorSupplier())
              .addMethod(getCreateTensorMethod())
              .addMethod(getCreateVariableMethod())
              .addMethod(getCreateEmbeddingMethod())
              .addMethod(getInitMethod())
              .addMethod(getLoadMethod())
              .addMethod(getSaveMethod())
              .addMethod(getPullMethod())
              .addMethod(getPushMethod())
              .addMethod(getReleaseMethod())
              .addMethod(getUpdateMethod())
              .addMethod(getSyncMethod())
              .addMethod(getCompleteTaskMethod())
              .build();
        }
      }
    }
    return result;
  }
}
