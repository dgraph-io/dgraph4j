package io.dgraph.client;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.14.0)",
    comments = "Source: graphresponse.proto")
public class DgraphGrpc {

  private DgraphGrpc() {}

  public static final String SERVICE_NAME = "graph.Dgraph";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<io.dgraph.client.Graphresponse.Request,
      io.dgraph.client.Graphresponse.Response> METHOD_QUERY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "graph.Dgraph", "Query"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.dgraph.client.Graphresponse.Request.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.dgraph.client.Graphresponse.Response.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DgraphStub newStub(io.grpc.Channel channel) {
    return new DgraphStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DgraphBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DgraphBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static DgraphFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DgraphFutureStub(channel);
  }

  /**
   */
  public static interface Dgraph {

    /**
     */
    public void query(io.dgraph.client.Graphresponse.Request request,
        io.grpc.stub.StreamObserver<io.dgraph.client.Graphresponse.Response> responseObserver);
  }

  @io.grpc.ExperimentalApi
  public static abstract class AbstractDgraph implements Dgraph, io.grpc.BindableService {

    @java.lang.Override
    public void query(io.dgraph.client.Graphresponse.Request request,
        io.grpc.stub.StreamObserver<io.dgraph.client.Graphresponse.Response> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return DgraphGrpc.bindService(this);
    }
  }

  /**
   */
  public static interface DgraphBlockingClient {

    /**
     */
    public io.dgraph.client.Graphresponse.Response query(io.dgraph.client.Graphresponse.Request request);
  }

  /**
   */
  public static interface DgraphFutureClient {

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.dgraph.client.Graphresponse.Response> query(
        io.dgraph.client.Graphresponse.Request request);
  }

  public static class DgraphStub extends io.grpc.stub.AbstractStub<DgraphStub>
      implements Dgraph {
    private DgraphStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DgraphStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DgraphStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DgraphStub(channel, callOptions);
    }

    @java.lang.Override
    public void query(io.dgraph.client.Graphresponse.Request request,
        io.grpc.stub.StreamObserver<io.dgraph.client.Graphresponse.Response> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_QUERY, getCallOptions()), request, responseObserver);
    }
  }

  public static class DgraphBlockingStub extends io.grpc.stub.AbstractStub<DgraphBlockingStub>
      implements DgraphBlockingClient {
    private DgraphBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DgraphBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DgraphBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DgraphBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public io.dgraph.client.Graphresponse.Response query(io.dgraph.client.Graphresponse.Request request) {
      return blockingUnaryCall(
          getChannel(), METHOD_QUERY, getCallOptions(), request);
    }
  }

  public static class DgraphFutureStub extends io.grpc.stub.AbstractStub<DgraphFutureStub>
      implements DgraphFutureClient {
    private DgraphFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DgraphFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DgraphFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DgraphFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<io.dgraph.client.Graphresponse.Response> query(
        io.dgraph.client.Graphresponse.Request request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_QUERY, getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final Dgraph serviceImpl;
    private final int methodId;

    public MethodHandlers(Dgraph serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_QUERY:
          serviceImpl.query((io.dgraph.client.Graphresponse.Request) request,
              (io.grpc.stub.StreamObserver<io.dgraph.client.Graphresponse.Response>) responseObserver);
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

  public static io.grpc.ServerServiceDefinition bindService(
      final Dgraph serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_QUERY,
          asyncUnaryCall(
            new MethodHandlers<
              io.dgraph.client.Graphresponse.Request,
              io.dgraph.client.Graphresponse.Response>(
                serviceImpl, METHODID_QUERY)))
        .build();
  }
}
