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
  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterReq,
      com.tencent.client.master.protos.RegisterResp> getRegisterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Register",
      requestType = com.tencent.client.master.protos.RegisterReq.class,
      responseType = com.tencent.client.master.protos.RegisterResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterReq,
      com.tencent.client.master.protos.RegisterResp> getRegisterMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.RegisterReq, com.tencent.client.master.protos.RegisterResp> getRegisterMethod;
    if ((getRegisterMethod = AngelCleintMasterGrpc.getRegisterMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getRegisterMethod = AngelCleintMasterGrpc.getRegisterMethod) == null) {
          AngelCleintMasterGrpc.getRegisterMethod = getRegisterMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.RegisterReq, com.tencent.client.master.protos.RegisterResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "Register"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.RegisterResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("Register"))
                  .build();
          }
        }
     }
     return getRegisterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq,
      com.tencent.client.master.protos.SetAngelLocationResp> getSetAngelLocationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetAngelLocation",
      requestType = com.tencent.client.master.protos.SetAngelLocationReq.class,
      responseType = com.tencent.client.master.protos.SetAngelLocationResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq,
      com.tencent.client.master.protos.SetAngelLocationResp> getSetAngelLocationMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.SetAngelLocationReq, com.tencent.client.master.protos.SetAngelLocationResp> getSetAngelLocationMethod;
    if ((getSetAngelLocationMethod = AngelCleintMasterGrpc.getSetAngelLocationMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getSetAngelLocationMethod = AngelCleintMasterGrpc.getSetAngelLocationMethod) == null) {
          AngelCleintMasterGrpc.getSetAngelLocationMethod = getSetAngelLocationMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.SetAngelLocationReq, com.tencent.client.master.protos.SetAngelLocationResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "SetAngelLocation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.SetAngelLocationReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.SetAngelLocationResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("SetAngelLocation"))
                  .build();
          }
        }
     }
     return getSetAngelLocationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetAngelLocationReq,
      com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAngelLocation",
      requestType = com.tencent.client.master.protos.GetAngelLocationReq.class,
      responseType = com.tencent.client.master.protos.GetAngelLocationResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetAngelLocationReq,
      com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.GetAngelLocationReq, com.tencent.client.master.protos.GetAngelLocationResp> getGetAngelLocationMethod;
    if ((getGetAngelLocationMethod = AngelCleintMasterGrpc.getGetAngelLocationMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getGetAngelLocationMethod = AngelCleintMasterGrpc.getGetAngelLocationMethod) == null) {
          AngelCleintMasterGrpc.getGetAngelLocationMethod = getGetAngelLocationMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.GetAngelLocationReq, com.tencent.client.master.protos.GetAngelLocationResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "GetAngelLocation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.GetAngelLocationReq.getDefaultInstance()))
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

  private static volatile io.grpc.MethodDescriptor<com.tencent.client.master.protos.SyncReq,
      com.tencent.client.master.protos.SyncResp> getSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Sync",
      requestType = com.tencent.client.master.protos.SyncReq.class,
      responseType = com.tencent.client.master.protos.SyncResp.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.tencent.client.master.protos.SyncReq,
      com.tencent.client.master.protos.SyncResp> getSyncMethod() {
    io.grpc.MethodDescriptor<com.tencent.client.master.protos.SyncReq, com.tencent.client.master.protos.SyncResp> getSyncMethod;
    if ((getSyncMethod = AngelCleintMasterGrpc.getSyncMethod) == null) {
      synchronized (AngelCleintMasterGrpc.class) {
        if ((getSyncMethod = AngelCleintMasterGrpc.getSyncMethod) == null) {
          AngelCleintMasterGrpc.getSyncMethod = getSyncMethod = 
              io.grpc.MethodDescriptor.<com.tencent.client.master.protos.SyncReq, com.tencent.client.master.protos.SyncResp>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ClientMaster.AngelCleintMaster", "Sync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.SyncReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tencent.client.master.protos.SyncResp.getDefaultInstance()))
                  .setSchemaDescriptor(new AngelCleintMasterMethodDescriptorSupplier("Sync"))
                  .build();
          }
        }
     }
     return getSyncMethod;
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
    public void register(com.tencent.client.master.protos.RegisterReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterResp> responseObserver) {
      asyncUnimplementedUnaryCall(getRegisterMethod(), responseObserver);
    }

    /**
     */
    public void setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SetAngelLocationResp> responseObserver) {
      asyncUnimplementedUnaryCall(getSetAngelLocationMethod(), responseObserver);
    }

    /**
     */
    public void getAngelLocation(com.tencent.client.master.protos.GetAngelLocationReq request,
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
    public void sync(com.tencent.client.master.protos.SyncReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SyncResp> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getRegisterMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.RegisterReq,
                com.tencent.client.master.protos.RegisterResp>(
                  this, METHODID_REGISTER)))
          .addMethod(
            getSetAngelLocationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.SetAngelLocationReq,
                com.tencent.client.master.protos.SetAngelLocationResp>(
                  this, METHODID_SET_ANGEL_LOCATION)))
          .addMethod(
            getGetAngelLocationMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.GetAngelLocationReq,
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
            getSyncMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.tencent.client.master.protos.SyncReq,
                com.tencent.client.master.protos.SyncResp>(
                  this, METHODID_SYNC)))
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
    public void register(com.tencent.client.master.protos.RegisterReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRegisterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SetAngelLocationResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetAngelLocationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAngelLocation(com.tencent.client.master.protos.GetAngelLocationReq request,
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
    public void sync(com.tencent.client.master.protos.SyncReq request,
        io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SyncResp> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
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
    public com.tencent.client.master.protos.RegisterResp register(com.tencent.client.master.protos.RegisterReq request) {
      return blockingUnaryCall(
          getChannel(), getRegisterMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.SetAngelLocationResp setAngelLocation(com.tencent.client.master.protos.SetAngelLocationReq request) {
      return blockingUnaryCall(
          getChannel(), getSetAngelLocationMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.tencent.client.master.protos.GetAngelLocationResp getAngelLocation(com.tencent.client.master.protos.GetAngelLocationReq request) {
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
    public com.tencent.client.master.protos.SyncResp sync(com.tencent.client.master.protos.SyncReq request) {
      return blockingUnaryCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.RegisterResp> register(
        com.tencent.client.master.protos.RegisterReq request) {
      return futureUnaryCall(
          getChannel().newCall(getRegisterMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.SetAngelLocationResp> setAngelLocation(
        com.tencent.client.master.protos.SetAngelLocationReq request) {
      return futureUnaryCall(
          getChannel().newCall(getSetAngelLocationMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.GetAngelLocationResp> getAngelLocation(
        com.tencent.client.master.protos.GetAngelLocationReq request) {
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
    public com.google.common.util.concurrent.ListenableFuture<com.tencent.client.master.protos.SyncResp> sync(
        com.tencent.client.master.protos.SyncReq request) {
      return futureUnaryCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER = 0;
  private static final int METHODID_SET_ANGEL_LOCATION = 1;
  private static final int METHODID_GET_ANGEL_LOCATION = 2;
  private static final int METHODID_HEART_BEAT = 3;
  private static final int METHODID_SYNC = 4;

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
        case METHODID_REGISTER:
          serviceImpl.register((com.tencent.client.master.protos.RegisterReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.RegisterResp>) responseObserver);
          break;
        case METHODID_SET_ANGEL_LOCATION:
          serviceImpl.setAngelLocation((com.tencent.client.master.protos.SetAngelLocationReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SetAngelLocationResp>) responseObserver);
          break;
        case METHODID_GET_ANGEL_LOCATION:
          serviceImpl.getAngelLocation((com.tencent.client.master.protos.GetAngelLocationReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.GetAngelLocationResp>) responseObserver);
          break;
        case METHODID_HEART_BEAT:
          serviceImpl.heartBeat((com.tencent.client.master.protos.HeartBeatReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.HeartBeatResp>) responseObserver);
          break;
        case METHODID_SYNC:
          serviceImpl.sync((com.tencent.client.master.protos.SyncReq) request,
              (io.grpc.stub.StreamObserver<com.tencent.client.master.protos.SyncResp>) responseObserver);
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
              .addMethod(getRegisterMethod())
              .addMethod(getSetAngelLocationMethod())
              .addMethod(getGetAngelLocationMethod())
              .addMethod(getHeartBeatMethod())
              .addMethod(getSyncMethod())
              .build();
        }
      }
    }
    return result;
  }
}
