/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static java.util.Arrays.asList;

import com.google.protobuf.InvalidProtocolBufferException;
import io.dgraph.DgraphProto.Payload;
import io.dgraph.DgraphProto.TxnContext;
import io.dgraph.DgraphProto.Version;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronous implementation of a Dgraph client using grpc.
 *
 * <p>Queries, mutations, and most other types of admin tasks can be run from the client.
 *
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 * @author Michail Klimenkov
 */
public class DgraphAsyncClient {
  private static final Logger LOG = LoggerFactory.getLogger(DgraphAsyncClient.class);
  private final List<DgraphGrpc.DgraphStub> stubs;
  private final Executor executor;
  private final ReadWriteLock jwtLock;
  private DgraphProto.Jwt jwt;

  /**
   * Creates a new client for interacting with a Dgraph store.
   *
   * <p>A single client is thread safe.
   *
   * @param stubs - an array of grpc stubs to be used by this client. The stubs to be used are
   *     chosen at random per transaction.
   */
  public DgraphAsyncClient(DgraphGrpc.DgraphStub... stubs) {
    this.stubs = asList(stubs);
    this.executor = ForkJoinPool.commonPool();
    this.jwtLock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a new client for interacting with a Dgraph store.
   *
   * <p>A single client is thread safe.
   *
   * @param executor - the executor to use for various asynchronous tasks executed by this client.
   * @param stubs - an array of grpc stubs to be used by this client. The stubs to be used are
   *     chosen at random per transaction.
   */
  public DgraphAsyncClient(Executor executor, DgraphGrpc.DgraphStub... stubs) {
    this.stubs = asList(stubs);
    this.executor = executor;
    this.jwtLock = new ReentrantReadWriteLock();
  }

  /**
   * login sends a LoginRequest to the server using the given userid and password for the default
   * namespace (0). If the LoginRequest is processed successfully, the response returned by the
   * server will contain an access JWT and a refresh JWT, which will be stored in the jwt field of
   * this class, and used for authorizing all subsequent requests sent to the server.
   *
   * @param userid the id of the user who is trying to login, e.g. Alice
   * @param password the password of the user
   * @return a future which can be used to wait for completion of the login request
   */
  public CompletableFuture<Void> login(String userid, String password) {
    return this.loginIntoNamespace(userid, password, 0L);
  }

  /**
   * loginIntoNamespace sends a LoginRequest to the server using the given userid, password and
   * namespace. If the LoginRequest is processed successfully, the response returned by the server
   * will contain an access JWT and a refresh JWT, which will be stored in the jwt field of this
   * class, and used for authorizing all subsequent requests sent to the server.
   *
   * @param userid the id of the user who is trying to login, e.g. Alice
   * @param password the password of the user
   * @param namespace the namespace in which to login
   * @return a future which can be used to wait for completion of the login request
   */
  public CompletableFuture<Void> loginIntoNamespace(
      String userid, String password, long namespace) {
    Lock wlock = jwtLock.writeLock();
    wlock.lock();
    try {
      final DgraphGrpc.DgraphStub client = anyClient();
      final DgraphProto.LoginRequest loginRequest =
          DgraphProto.LoginRequest.newBuilder()
              .setUserid(userid)
              .setPassword(password)
              .setNamespace(namespace)
              .build();

      StreamObserverBridge<DgraphProto.Response> bridge = new StreamObserverBridge<>();
      client.login(loginRequest, bridge);
      return bridge
          .getDelegate()
          .thenAccept(
              (DgraphProto.Response response) -> {
                try {
                  // set the jwt field
                  jwt = DgraphProto.Jwt.parseFrom(response.getJson());
                } catch (InvalidProtocolBufferException e) {
                  String errmsg = "error while parsing jwt from the response: ";
                  LOG.error(errmsg, e);
                  throw new RuntimeException(errmsg, e);
                }
              });
    } finally {
      wlock.unlock();
    }
  }

  protected CompletableFuture<Void> retryLogin() {
    Lock wlock = jwtLock.writeLock();
    wlock.lock();
    try {
      if (jwt.getRefreshJwt().isEmpty()) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("refresh JWT should not be empty"));
        return future;
      }

      final DgraphGrpc.DgraphStub client = anyClient();
      final DgraphProto.LoginRequest loginRequest =
          DgraphProto.LoginRequest.newBuilder().setRefreshToken(jwt.getRefreshJwt()).build();

      StreamObserverBridge<DgraphProto.Response> bridge = new StreamObserverBridge<>();
      client.login(loginRequest, bridge);
      return bridge
          .getDelegate()
          .thenAccept(
              (DgraphProto.Response response) -> {
                try {
                  // set the jwt field
                  jwt = DgraphProto.Jwt.parseFrom(response.getJson());
                } catch (InvalidProtocolBufferException e) {
                  LOG.error("error while parsing jwt from the response: ", e);
                }
              });
    } finally {
      wlock.unlock();
    }
  }

  /**
   * getStubWithJwt adds an AttachHeadersInterceptor to the stub, which will eventually attach a
   * header whose key is accessJwt and value is the access JWT stored in the current
   * DgraphAsyncClient object.
   *
   * @param stub the original stub that we should attach JWT to
   * @return the augmented stub with JWT
   */
  protected DgraphGrpc.DgraphStub getStubWithJwt(DgraphGrpc.DgraphStub stub) {
    Lock readLock = jwtLock.readLock();
    readLock.lock();
    try {
      if (jwt != null && !jwt.getAccessJwt().isEmpty()) {
        Metadata metadata = new Metadata();
        metadata.put(
            Metadata.Key.of("accessJwt", Metadata.ASCII_STRING_MARSHALLER), jwt.getAccessJwt());
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
      }

      return stub;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * runWithRetries takes a supplier of CompletableFuture, tries to get the result from it while
   * handling exceptions caused by access JWT expiration. If such an exception happens,
   * runWithRetries will retry login using the refresh JWT and retry the logic in the supplier.
   *
   * @param <T> The type of the supplier's returned CompletableFuture. If the supplier provides
   *     logic to run queries, then the type T will be DgraphProto.Response.
   * @param operation the name of the operation
   * @param callable the callable returning a CompletableFuture, which encapsulates the logic to run
   *     queries, mutations or alter operations
   * @return a completable future which can be used to get the result
   */
  protected <T> CompletableFuture<T> runWithRetries(
      String operation, Callable<CompletableFuture<T>> callable) {
    final Callable<CompletableFuture<T>> ctxCallable = Context.current().wrap(callable);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return ctxCallable.call().get();
          } catch (InterruptedException e) {
            LOG.error("The " + operation + " got interrupted:", e);
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            if (ExceptionUtil.isJwtExpired(e.getCause())) {
              try {
                // retry the login
                retryLogin().get();
                // retry the supplied logic
                return ctxCallable.call().get();
              } catch (InterruptedException ie) {
                LOG.error("The retried " + operation + " got interrupted:", ie);
                throw new RuntimeException(ie);
              } catch (ExecutionException ie) {
                LOG.error("The retried " + operation + " encounters an execution exception:", ie);
                throw new RuntimeException(ie);
              } catch (Exception ie) {
                LOG.error("The retried " + operation + " encounters a completion exception:", ie);
                throw new CompletionException(ie);
              }
            } else if (e.getCause() instanceof StatusRuntimeException) {
              StatusRuntimeException ex1 = (StatusRuntimeException) e.getCause();
              Status.Code code = ex1.getStatus().getCode();
              String desc = ex1.getStatus().getDescription();

              if (code.equals(Status.Code.ABORTED)
                  || code.equals(Status.Code.FAILED_PRECONDITION)) {
                throw new CompletionException(new TxnConflictException(desc));
              }
            }
            // Handle the case when the outer exception is not caused by JWT expiration
            throw new RuntimeException(
                "The " + operation + " encountered an execution exception:", e);
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        },
        this.executor);
  }

  /**
   * Alter can be used to perform the following operations, by setting the right fields in the
   * protocol buffer Operation object.
   *
   * <p>- Modify a schema.
   *
   * <p>- Drop predicate.
   *
   * <p>- Drop the database.
   *
   * @param op a protocol buffer Operation object representing the operation being performed.
   * @return CompletableFuture with instance of Payload set as result
   */
  public CompletableFuture<Payload> alter(DgraphProto.Operation op) {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "alter",
        () -> {
          StreamObserverBridge<Payload> observerBridge = new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.alter(op, observerBridge);
          return observerBridge.getDelegate();
        });
  }

  /**
   * checkVersion can be used to find out the version of the Dgraph instance this client is
   * interacting with.
   *
   * @return A CompletableFuture containing the Version object which represents the version of
   *     Dgraph instance.
   */
  public CompletableFuture<Version> checkVersion() {
    final DgraphGrpc.DgraphStub stub = anyClient();
    final DgraphProto.Check checkRequest = DgraphProto.Check.newBuilder().build();

    return runWithRetries(
        "checkVersion",
        () -> {
          StreamObserverBridge<Version> observerBridge = new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.checkVersion(checkRequest, observerBridge);
          return observerBridge.getDelegate();
        });
  }

  // ---------------------------------------------------------------------------
  // DQL
  // ---------------------------------------------------------------------------

  /**
   * Runs a DQL query or mutation using the RunDQL RPC.
   *
   * @param request a fully-built RunDQLRequest
   * @return CompletableFuture with the Response
   */
  public CompletableFuture<DgraphProto.Response> runDQL(DgraphProto.RunDQLRequest request) {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "runDQL",
        () -> {
          StreamObserverBridge<DgraphProto.Response> bridge = new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.runDQL(request, bridge);
          return bridge.getDelegate();
        });
  }

  /**
   * Runs a DQL query or mutation with default options.
   *
   * @param dqlQuery the DQL query string
   * @return CompletableFuture with the Response
   */
  public CompletableFuture<DgraphProto.Response> runDQL(String dqlQuery) {
    return runDQL(dqlQuery, Collections.emptyMap(), false, false);
  }

  /**
   * Runs a DQL query or mutation with full control over options.
   *
   * @param dqlQuery the DQL query string
   * @param vars query variables (may be empty)
   * @param readOnly whether the query is read-only
   * @param bestEffort whether to use best-effort reads
   * @return CompletableFuture with the Response
   */
  public CompletableFuture<DgraphProto.Response> runDQL(
      String dqlQuery, Map<String, String> vars, boolean readOnly, boolean bestEffort) {
    DgraphProto.RunDQLRequest.Builder builder =
        DgraphProto.RunDQLRequest.newBuilder().setDqlQuery(dqlQuery);
    if (vars != null && !vars.isEmpty()) {
      builder.putAllVars(vars);
    }
    builder.setReadOnly(readOnly);
    builder.setBestEffort(bestEffort);
    return runDQL(builder.build());
  }

  // ---------------------------------------------------------------------------
  // ID / Timestamp / Namespace Allocation
  // ---------------------------------------------------------------------------

  /**
   * Allocates IDs of the given type from the Dgraph cluster.
   *
   * @param request a fully-built AllocateIDsRequest
   * @return CompletableFuture with the AllocateIDsResponse containing start/end range
   */
  public CompletableFuture<DgraphProto.AllocateIDsResponse> allocateIDs(
      DgraphProto.AllocateIDsRequest request) {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "allocateIDs",
        () -> {
          StreamObserverBridge<DgraphProto.AllocateIDsResponse> bridge =
              new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.allocateIDs(request, bridge);
          return bridge.getDelegate();
        });
  }

  /**
   * Allocates a range of UIDs from the Dgraph cluster.
   *
   * @param howMany the number of UIDs to allocate (must be &gt; 0)
   * @return CompletableFuture with the AllocateIDsResponse containing start/end range
   */
  public CompletableFuture<DgraphProto.AllocateIDsResponse> allocateUIDs(long howMany) {
    if (howMany <= 0) {
      throw new IllegalArgumentException("howMany must be greater than 0");
    }
    return allocateIDs(
        DgraphProto.AllocateIDsRequest.newBuilder()
            .setHowMany(howMany)
            .setLeaseType(DgraphProto.LeaseType.UID)
            .build());
  }

  /**
   * Allocates a range of timestamps from the Dgraph cluster.
   *
   * @param howMany the number of timestamps to allocate (must be &gt; 0)
   * @return CompletableFuture with the AllocateIDsResponse containing start/end range
   */
  public CompletableFuture<DgraphProto.AllocateIDsResponse> allocateTimestamps(long howMany) {
    if (howMany <= 0) {
      throw new IllegalArgumentException("howMany must be greater than 0");
    }
    return allocateIDs(
        DgraphProto.AllocateIDsRequest.newBuilder()
            .setHowMany(howMany)
            .setLeaseType(DgraphProto.LeaseType.TS)
            .build());
  }

  /**
   * Allocates a range of namespace IDs from the Dgraph cluster.
   *
   * @param howMany the number of namespace IDs to allocate (must be &gt; 0)
   * @return CompletableFuture with the AllocateIDsResponse containing start/end range
   */
  public CompletableFuture<DgraphProto.AllocateIDsResponse> allocateNamespaces(long howMany) {
    if (howMany <= 0) {
      throw new IllegalArgumentException("howMany must be greater than 0");
    }
    return allocateIDs(
        DgraphProto.AllocateIDsRequest.newBuilder()
            .setHowMany(howMany)
            .setLeaseType(DgraphProto.LeaseType.NS)
            .build());
  }

  // ---------------------------------------------------------------------------
  // Namespace Management
  // ---------------------------------------------------------------------------

  /**
   * Creates a new namespace in the Dgraph cluster.
   *
   * @return CompletableFuture with the CreateNamespaceResponse containing the new namespace ID
   */
  public CompletableFuture<DgraphProto.CreateNamespaceResponse> createNamespace() {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "createNamespace",
        () -> {
          StreamObserverBridge<DgraphProto.CreateNamespaceResponse> bridge =
              new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.createNamespace(
              DgraphProto.CreateNamespaceRequest.newBuilder().build(), bridge);
          return bridge.getDelegate();
        });
  }

  /**
   * Drops (deletes) a namespace from the Dgraph cluster.
   *
   * @param namespace the namespace ID to drop
   * @return CompletableFuture with the DropNamespaceResponse
   */
  public CompletableFuture<DgraphProto.DropNamespaceResponse> dropNamespace(long namespace) {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "dropNamespace",
        () -> {
          StreamObserverBridge<DgraphProto.DropNamespaceResponse> bridge =
              new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.dropNamespace(
              DgraphProto.DropNamespaceRequest.newBuilder().setNamespace(namespace).build(),
              bridge);
          return bridge.getDelegate();
        });
  }

  /**
   * Lists all namespaces in the Dgraph cluster.
   *
   * @return CompletableFuture with the ListNamespacesResponse
   */
  public CompletableFuture<DgraphProto.ListNamespacesResponse> listNamespaces() {
    final DgraphGrpc.DgraphStub stub = anyClient();

    return runWithRetries(
        "listNamespaces",
        () -> {
          StreamObserverBridge<DgraphProto.ListNamespacesResponse> bridge =
              new StreamObserverBridge<>();
          DgraphGrpc.DgraphStub localStub = getStubWithJwt(stub);
          localStub.listNamespaces(
              DgraphProto.ListNamespacesRequest.newBuilder().build(), bridge);
          return bridge.getDelegate();
        });
  }

  // ---------------------------------------------------------------------------
  // Convenience Alter Methods
  // ---------------------------------------------------------------------------

  /**
   * Drops all data and schema from the Dgraph instance.
   *
   * @return CompletableFuture with Payload
   */
  public CompletableFuture<Payload> dropAll() {
    return alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());
  }

  /**
   * Drops all data but preserves the schema.
   *
   * @return CompletableFuture with Payload
   */
  public CompletableFuture<Payload> dropData() {
    return alter(
        DgraphProto.Operation.newBuilder()
            .setDropOp(DgraphProto.Operation.DropOp.DATA)
            .build());
  }

  /**
   * Drops a single predicate (attribute) from the schema and removes all its data.
   *
   * @param predicate the name of the predicate to drop
   * @return CompletableFuture with Payload
   */
  public CompletableFuture<Payload> dropPredicate(String predicate) {
    if (predicate == null || predicate.isEmpty()) {
      throw new IllegalArgumentException("predicate must not be null or empty");
    }
    return alter(
        DgraphProto.Operation.newBuilder()
            .setDropOp(DgraphProto.Operation.DropOp.ATTR)
            .setDropValue(predicate)
            .build());
  }

  /**
   * Drops a type from the schema.
   *
   * @param typeName the name of the type to drop
   * @return CompletableFuture with Payload
   */
  public CompletableFuture<Payload> dropType(String typeName) {
    if (typeName == null || typeName.isEmpty()) {
      throw new IllegalArgumentException("typeName must not be null or empty");
    }
    return alter(
        DgraphProto.Operation.newBuilder()
            .setDropOp(DgraphProto.Operation.DropOp.TYPE)
            .setDropValue(typeName)
            .build());
  }

  /**
   * Sets the schema on the Dgraph instance.
   *
   * @param schema the schema definition string
   * @return CompletableFuture with Payload
   */
  public CompletableFuture<Payload> setSchema(String schema) {
    if (schema == null || schema.isEmpty()) {
      throw new IllegalArgumentException("schema must not be null or empty");
    }
    return alter(DgraphProto.Operation.newBuilder().setSchema(schema).build());
  }

  private DgraphGrpc.DgraphStub anyClient() {
    int index = ThreadLocalRandom.current().nextInt(stubs.size());
    DgraphGrpc.DgraphStub rawStub = stubs.get(index);
    return getStubWithJwt(rawStub);
  }

  /**
   * Creates a new AsyncTransaction object. All operations performed by this transaction are
   * asynchronous.
   *
   * <p>A transaction lifecycle is as follows:
   *
   * <p>- Created using AsyncTransaction#newTransaction()
   *
   * <p>- Various AsyncTransaction#query() and AsyncTransaction#mutate() calls made.
   *
   * <p>- Commit using AsyncTransacation#commit() or Discard using AsyncTransaction#discard(). If
   * any mutations have been made, It's important that at least one of these methods is called to
   * clean up resources. Discard is a no-op if Commit has already been called, so it's safe to call
   * it after Commit.
   *
   * @return a new AsyncTransaction object.
   */
  public AsyncTransaction newTransaction() {
    return new AsyncTransaction(this, this.anyClient());
  }

  /**
   * Creates a new AsyncTransaction object from a TxnContext. All operations performed by this
   * transaction are asynchronous.
   *
   * <p>A transaction lifecycle is as follows:
   *
   * <p>- Created using AsyncTransaction#newTransaction()
   *
   * <p>- Various AsyncTransaction#query() and AsyncTransaction#mutate() calls made.
   *
   * <p>- Commit using AsyncTransaction#commit() or Discard using AsyncTransaction#discard(). If any
   * mutations have been made, It's important that at least one of these methods is called to clean
   * up resources. Discard is a no-op if Commit has already been called, so it's safe to call it
   * after Commit.
   *
   * @return a new AsyncTransaction object.
   */
  public AsyncTransaction newTransaction(TxnContext context) {
    return new AsyncTransaction(this, this.anyClient(), context);
  }

  /**
   * Creates a new AsyncTransaction object that only allows queries. Any AsyncTransaction#mutate()
   * or AsyncTransaction#commit() call made to the read only transaction will result in
   * TxnReadOnlyException. All operations performed by this transaction are asynchronous.
   *
   * @return a new AsyncTransaction object
   */
  public AsyncTransaction newReadOnlyTransaction() {
    return new AsyncTransaction(this, this.anyClient(), true);
  }

  /**
   * Creates a new AsyncTransaction object from a TnxContext that only allows queries. Any
   * AsyncTransaction#mutate() or AsyncTransaction#commit() call made to the read only transaction
   * will result in TxnReadOnlyException. All operations performed by this transaction are
   * asynchronous.
   *
   * @return a new AsyncTransaction object
   */
  public AsyncTransaction newReadOnlyTransaction(TxnContext context) {
    return new AsyncTransaction(this, this.anyClient(), context, true);
  }

  /** Calls %{@link io.grpc.ManagedChannel#shutdown} on all connections for this client */
  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              for (DgraphGrpc.DgraphStub stub : this.stubs) {
                Channel chan = stub.getChannel();
                if (chan instanceof ManagedChannel) {
                  ((ManagedChannel) chan).shutdown();
                }
              }
            },
            this.executor);
    return future;
  }
}
