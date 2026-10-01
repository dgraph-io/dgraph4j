/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.dgraph.DgraphProto.Jwt;
import io.dgraph.DgraphProto.LoginRequest;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Payload;
import io.dgraph.DgraphProto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Asserts what the server actually receives in the {@code accessJwt} header. Runs against an
 * in-process fake Dgraph, so it needs no cluster.
 */
public class AccessJwtHeaderTest {

  private static final Metadata.Key<String> ACCESS_JWT =
      Metadata.Key.of("accessJwt", Metadata.ASCII_STRING_MARSHALLER);
  private static final Operation DROP_ALL = Operation.newBuilder().setDropAll(true).build();
  private static final long TIMEOUT_SECONDS = 10;

  private FakeDgraph service;
  private HeaderRecorder recorder;
  private Server server;
  private ManagedChannel channel;
  private ExecutorService executor;
  private DgraphAsyncClient client;

  @BeforeMethod
  public void setUp() throws Exception {
    service = new FakeDgraph();
    recorder = new HeaderRecorder();
    server = ServerBuilder.forPort(0).addService(service).intercept(recorder).build().start();
    channel =
        ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
    executor = Executors.newFixedThreadPool(4);
    client = new DgraphAsyncClient(executor, DgraphGrpc.newStub(channel));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    channel.shutdownNow();
    server.shutdownNow();
    executor.shutdownNow();
    channel.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    server.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void authenticatedRequestCarriesExactlyOneAccessJwt() throws Exception {
    login();
    client.alter(DROP_ALL).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(
        recorder.calls("Alter"),
        Collections.singletonList(Collections.singletonList("access-1")),
        "the server must see exactly one accessJwt header, holding the current token");
  }

  @Test
  public void loginCarriesNoAccessJwt() throws Exception {
    login();
    client.alter(DROP_ALL).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    login();

    List<List<String>> logins = recorder.calls("Login");
    assertEquals(logins.size(), 2);
    assertTrue(
        logins.get(1).isEmpty(), "a login must not present the token it is about to replace");
  }

  // The regression test: before anyClient() stopped attaching the JWT, the retried call carried
  // both the refreshed and the expired token, and only Dgraph's read of index 0 kept it working.
  @Test
  public void expiryRetryPresentsOnlyTheRefreshedToken() throws Exception {
    service.expireFirstAlter = true;
    login();

    client.alter(DROP_ALL).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertEquals(
        recorder.calls("Alter"),
        Arrays.asList(
            Collections.singletonList("access-1"), Collections.singletonList("access-2")),
        "the retry must present the refreshed token and nothing else");
  }

  private void login() throws Exception {
    client.login("groot", "password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /** Answers login with a fresh numbered JWT and can fail the first alter as token-expired. */
  private static final class FakeDgraph extends DgraphGrpc.DgraphImplBase {
    private final AtomicInteger logins = new AtomicInteger();
    private final AtomicInteger alters = new AtomicInteger();
    private volatile boolean expireFirstAlter;

    @Override
    public void login(LoginRequest request, StreamObserver<Response> observer) {
      String n = String.valueOf(logins.incrementAndGet());
      Jwt jwt =
          Jwt.newBuilder().setAccessJwt("access-" + n).setRefreshJwt("refresh-" + n).build();
      observer.onNext(Response.newBuilder().setJson(jwt.toByteString()).build());
      observer.onCompleted();
    }

    @Override
    public void alter(Operation request, StreamObserver<Payload> observer) {
      if (alters.incrementAndGet() == 1 && expireFirstAlter) {
        observer.onError(
            Status.UNAUTHENTICATED.withDescription("Token is expired").asRuntimeException());
        return;
      }
      observer.onNext(Payload.getDefaultInstance());
      observer.onCompleted();
    }
  }

  /** Records the accessJwt values of every inbound call, in arrival order, per method. */
  private static final class HeaderRecorder implements ServerInterceptor {
    private final Map<String, List<List<String>>> byMethod = new ConcurrentHashMap<>();

    @Override
    public <R, S> ServerCall.Listener<R> interceptCall(
        ServerCall<R, S> call, Metadata headers, ServerCallHandler<R, S> next) {
      List<String> tokens = new ArrayList<>();
      Iterable<String> values = headers.getAll(ACCESS_JWT);
      if (values != null) {
        values.forEach(tokens::add);
      }
      byMethod
          .computeIfAbsent(
              method(call), key -> Collections.synchronizedList(new ArrayList<>()))
          .add(tokens);
      return next.startCall(call, headers);
    }

    private static String method(ServerCall<?, ?> call) {
      String full = call.getMethodDescriptor().getFullMethodName();
      return full.substring(full.lastIndexOf('/') + 1);
    }

    List<List<String>> calls(String method) {
      return byMethod.getOrDefault(method, Collections.emptyList());
    }
  }
}
