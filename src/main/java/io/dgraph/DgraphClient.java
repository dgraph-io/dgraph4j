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

import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.TxnContext;
import io.dgraph.DgraphProto.Version;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executor;

/**
 * Implementation of a DgraphClient using grpc.
 *
 * <p>Queries, mutations, and most other types of admin tasks can be run from the client.
 *
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 * @author Michail Klimenkov
 * @author Neeraj Battan
 * @author Abhimanyu Singh Gaur
 */
public class DgraphClient {
  private static final String gRPC_AUTHORIZATION_HEADER_NAME = "authorization";

  private final DgraphAsyncClient asyncClient;

  /**
   * Creates a gRPC stub that can be used to construct clients to connect with Slash GraphQL.
   *
   * @param slashEndpoint The url of the Slash GraphQL endpoint. Example:
   *     https://your-slash-instance.cloud.dgraph.io/graphql
   * @param apiKey The API key used to connect to your Slash GraphQL instance.
   * @return A new DgraphGrpc.DgraphStub object to be used with DgraphClient/DgraphAsyncClient.
   */
  public static DgraphGrpc.DgraphStub clientStubFromSlashEndpoint(
      String slashEndpoint, String apiKey) throws MalformedURLException {
    String[] parts = new URL(slashEndpoint).getHost().split("[.]", 2);
    if (parts.length < 2) {
      throw new MalformedURLException("Invalid Slash URL.");
    }
    String gRPCAddress = parts[0] + ".grpc." + parts[1];

    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of(gRPC_AUTHORIZATION_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER), apiKey);
    return MetadataUtils.attachHeaders(
        DgraphGrpc.newStub(
            ManagedChannelBuilder.forAddress(gRPCAddress, 443).useTransportSecurity().build()),
        metadata);
  }

  /**
   * Creates a new client for interacting with a Dgraph store.
   *
   * <p>A single client is thread safe.
   *
   * @param stubs - an array of grpc stubs to be used by this client. The stubs to be used are
   *     chosen at random per transaction.
   */
  public DgraphClient(DgraphGrpc.DgraphStub... stubs) {
    this.asyncClient = new DgraphAsyncClient(stubs);
  }

  /**
   * Creates a new client for interacting with a Dgraph store.
   *
   * <p>A single client is thread safe.
   *
   * @param executor - the executor to use for various asynchronous tasks executed by the underlying
   *     asynchronous client.
   * @param stubs - an array of grpc stubs to be used by this client. The stubs to be used are
   *     chosen at random per transaction.
   */
  public DgraphClient(Executor executor, DgraphGrpc.DgraphStub... stubs) {
    this.asyncClient = new DgraphAsyncClient(executor, stubs);
  }

  /**
   * Creates a new Transaction object. All operations performed by this transaction are synchronous.
   *
   * <p>A transaction lifecycle is as follows:
   *
   * <p>- Created using AsyncTransaction#newTransaction()
   *
   * <p>- Various AsyncTransaction#query() and AsyncTransaction#mutate() calls made.
   *
   * <p>- Commit using Transacation#commit() or Discard using AsyncTransaction#discard(). If any
   * mutations have been made, It's important that at least one of these methods is called to clean
   * up resources. Discard is a no-op if Commit has already been called, so it's safe to call it
   * after Commit.
   *
   * @return a new Transaction object.
   */
  public Transaction newTransaction() {
    return new Transaction(asyncClient.newTransaction());
  }

  /**
   * Creates a new Transaction object from a TxnContext. All operations performed by this
   * transaction are synchronous.
   *
   * <p>A transaction lifecycle is as follows:
   *
   * <p>- Created using AsyncTransaction#newTransaction()
   *
   * <p>- Various AsyncTransaction#query() and AsyncTransaction#mutate() calls made.
   *
   * <p>- Commit using Transacation#commit() or Discard using AsyncTransaction#discard(). If any
   * mutations have been made, It's important that at least one of these methods is called to clean
   * up resources. Discard is a no-op if Commit has already been called, so it's safe to call it
   * after Commit.
   *
   * @return a new Transaction object.
   */
  public Transaction newTransaction(TxnContext context) {
    return new Transaction(asyncClient.newTransaction(context));
  }

  /**
   * Creates a new AsyncTransaction object that only allows queries. Any Transaction#mutate() or
   * Transaction#commit() call made to the read only transaction will result in
   * TxnReadOnlyException. All operations performed by this transaction are synchronous.
   *
   * @return a new AsyncTransaction object
   */
  public Transaction newReadOnlyTransaction() {
    return new Transaction(asyncClient.newReadOnlyTransaction());
  }

  /**
   * Creates a new AsyncTransaction object from a TnxContext that only allows queries. Any
   * Transaction#mutate() or Transaction#commit() call made to the read only transaction will result
   * in TxnReadOnlyException. All operations performed by this transaction are synchronous.
   *
   * @return a new AsyncTransaction object
   */
  public Transaction newReadOnlyTransaction(TxnContext context) {
    return new Transaction(asyncClient.newReadOnlyTransaction(context));
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
   */
  public void alter(Operation op) {
    ExceptionUtil.withExceptionUnwrapped(
        () -> {
          asyncClient.alter(op).join();
        });
  }

  /**
   * checkVersion can be used to find out the version of the Dgraph instance this client is
   * interacting with.
   *
   * @return A Version object which represents the version of Dgraph instance.
   */
  public Version checkVersion() {
    return asyncClient.checkVersion().join();
  }

  /**
   * login sends a LoginRequest to the server that contains the userid and password. If the
   * LoginRequest is processed successfully, the response returned by the server will contain an
   * access JWT and a refresh JWT, which will be stored in the jwt field of this class, and used for
   * authorizing all subsequent requests sent to the server.
   *
   * @param userid the id of the user who is trying to login, e.g. Alice
   * @param password the password of the user
   */
  public void login(String userid, String password) {
    asyncClient.login(userid, password).join();
  }
}
