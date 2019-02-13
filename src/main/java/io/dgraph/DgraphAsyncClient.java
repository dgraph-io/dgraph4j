/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import static java.util.Arrays.asList;

import com.google.protobuf.InvalidProtocolBufferException;
import io.dgraph.DgraphProto.LinRead.Sequencing;
import io.dgraph.DgraphProto.Payload;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
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
    this.jwtLock = new ReentrantReadWriteLock();
  }

  /**
   * login sends a LoginRequest to the server that contains the userid and password. If the
   * LoginRequest is processed successfully, the response returned by the server will contain an
   * access JWT and a refresh JWT, which will be stored in the jwt field of this class, and used for
   * authorizing all subsequent requests sent to the server.
   *
   * @param userid the id of the user who is trying to login, e.g. Alice
   * @param password the password of the user
   * @return a future which can be used to wait for completion of the login request
   */
  public CompletableFuture<Void> login(String userid, String password) {
    Lock wlock = jwtLock.writeLock();
    wlock.lock();
    try {
      final DgraphGrpc.DgraphStub client = anyClient();
      final DgraphProto.LoginRequest loginRequest =
          DgraphProto.LoginRequest.newBuilder().setUserid(userid).setPassword(password).build();
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

  protected CompletableFuture<Void> retryLogin() {
    Lock rlock = jwtLock.readLock();
    rlock.lock();
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
      rlock.unlock();
    }
  }

  protected DgraphGrpc.DgraphStub getStubWithJwt(DgraphGrpc.DgraphStub stub) {
    Lock rlock = jwtLock.readLock();
    rlock.lock();
    try {
      if (jwt != null && !jwt.getAccessJwt().isEmpty()) {
        Metadata metadata = new Metadata();
        metadata.put(
            Metadata.Key.of("accessJwt", Metadata.ASCII_STRING_MARSHALLER), jwt.getAccessJwt());
        return MetadataUtils.attachHeaders(stub, metadata);
      }

      return stub;
    } finally {
      rlock.unlock();
    }
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
    final DgraphGrpc.DgraphStub client = anyClient();
    StreamObserverBridge<Payload> observerBridge = new StreamObserverBridge<>();
    client.alter(op, observerBridge);
    return observerBridge.getDelegate();
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
   * @param sequencing - the Sequencing strategy to be used
   * @return the new async transaction object
   * @deprecated the sequencing feature has been deprecated
   */
  public AsyncTransaction newTransaction(Sequencing sequencing) {
    return new AsyncTransaction(this, this.anyClient());
  }
}
