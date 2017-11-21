/*
 * Copyright 2016-17 DGraph Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dgraph;

import io.dgraph.DgraphProto.*;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a DgraphClient using grpc.
 *
 * <p>Queries, mutations, and most other types of admin tasks can be run from the client.
 *
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 */
public class DgraphClient {

  private static final Logger logger = LoggerFactory.getLogger(DgraphClient.class);

  private List<DgraphGrpc.DgraphBlockingStub> clients;

  private LinRead linRead;

  synchronized LinRead getLinRead() {
    return linRead;
  }

  synchronized void setLinRead(LinRead linRead) {
    this.linRead = linRead;
  }

  /**
   * Creates a new Dgraph for interacting with a Dgraph store.
   *
   * <p>A single client is thread safe.
   *
   * @param clients One or more synchronous grpc clients. Can contain connections to multiple
   *     servers in a cluster.
   */
  public DgraphClient(List<DgraphGrpc.DgraphBlockingStub> clients) {
    this.clients = clients;
    linRead = LinRead.getDefaultInstance();
  }

  /**
   * Creates a new Transaction object.
   *
   * <p>A transaction lifecycle is as follows:
   *
   * <p>- Created using Transaction#newTransaction()
   *
   * <p>- Various Transaction#query() and Transaction#mutate() calls made.
   *
   * <p>- Commit using Transacation#commit() or Discard using Transaction#discard(). If any
   * mutations have been made, It's important that at least one of these methods is called to clean
   * up resources. Discard is a no-op if Commit has already been called, so it's safe to call it
   * after Commit.
   *
   * @return a new Transaction object.
   */
  public Transaction newTransaction() {
    return new Transaction();
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
    final DgraphGrpc.DgraphBlockingStub client = anyClient();
    client.alter(op);
  }

  private DgraphGrpc.DgraphBlockingStub anyClient() {
    Random rand = new Random();
    return clients.get(rand.nextInt(clients.size()));
  }

  static LinRead mergeLinReads(LinRead dst, LinRead src) {
    LinRead.Builder result = LinRead.newBuilder(dst);
    for (Map.Entry<Integer, Long> entry : src.getIdsMap().entrySet()) {
      if (dst.containsIds(entry.getKey())
          && dst.getIdsOrThrow(entry.getKey()) >= entry.getValue()) {
        // Do nothing
      } else {
        result.putIds(entry.getKey(), entry.getValue());
      }
    }
    return result.build();
  }

  public class Transaction {
    TxnContext context;
    boolean finished;
    boolean mutated;

    Transaction() {
      context = TxnContext.newBuilder().setLinRead(DgraphClient.this.getLinRead()).build();
    }

    /**
     * sends a query to one of the connected dgraph instances. If no mutations need to be made in
     * the same transaction, it's convenient to chain the method: <code>
     * client.NewTransaction().query(...)</code>.
     *
     * @param query Query in GraphQL+-
     * @param vars variables referred to in the query.
     * @return a Response protocol buffer object.
     */
    public Response query(final String query, final Map<String, String> vars) {
      logger.debug("Starting query...");
      final Request request =
          Request.newBuilder()
              .setQuery(query)
              .putAllVars(vars)
              .setStartTs(context.getStartTs())
              .setLinRead(context.getLinRead())
              .build();
      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      logger.debug("Sending request to Dgraph...");
      final Response response = client.query(request);
      logger.debug("Received response from Dgraph!");
      mergeContext(response.getTxn());
      return response;
    }

    /**
     * Allows data stored on dgraph instances to be modified. The fields in Mutation come in pairs,
     * set and delete. Mutations can either be encoded as JSON or as RDFs.
     *
     * <p>If the commitNow property on the Mutation object is set,
     *
     * @param mutation a Mutation protocol buffer object representing the mutation.
     * @return an Assigned protocol buffer object. his call will result in the transaction being
     *     committed. In this case, an explicit call to Transaction#commit doesn't need to
     *     subsequently be made.
     */
    public Assigned mutate(Mutation mutation) {
      if (finished) {
        throw new TxnFinishedException();
      }

      Mutation request = Mutation.newBuilder(mutation).setStartTs(context.getStartTs()).build();

      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      Assigned ag = client.mutate(request);
      mutated = true;

      mergeContext(ag.getContext());

      if (!ag.getError().equals("")) {
        throw new DgraphException(ag.getError());
      }

      return ag;
    }

    /**
     * Commits any mutations that have been made in the transaction. Once Commit has been called,
     * the lifespan of the transaction is complete.
     *
     * <p>Errors could be thrown for various reasons. Notably, a StatusRuntimeException could be thrown
     * if transactions that modify the same data are being run concurrently. It's up to the user to
     * decide if they wish to retry. In this case, the user should create a new transaction.
     */
    public void commit() {
      if (finished) {
        throw new TxnFinishedException();
      }

      finished = true;

      if (!mutated) {
        return;
      }

      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      client.commitOrAbort(context);
    }

    /**
     * Cleans up the resources associated with an uncommitted transaction that contains mutations.
     * It is a no-op on transactions that have already been committed or don't contain mutations.
     * Therefore it is safe (and recommended) to call it in a finally block.
     *
     * <p>In some cases, the transaction can't be discarded, e.g. the grpc connection is
     * unavailable. In these cases, the server will eventually do the transaction clean up.
     */
    public void discard() {
      if (finished) {
        return;
      }
      finished = true;

      if (!mutated) {
        return;
      }

      context = TxnContext.newBuilder(context).setAborted(true).build();

      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      client.commitOrAbort(context);
    }

    private void mergeContext(final TxnContext src) {
      TxnContext.Builder result = TxnContext.newBuilder(context);

      LinRead lr = mergeLinReads(this.context.getLinRead(), src.getLinRead());
      result.setLinRead(lr);

      lr = mergeLinReads(DgraphClient.this.getLinRead(), lr);
      DgraphClient.this.setLinRead(lr);

      if (context.getStartTs() == 0) {
        result.setStartTs(src.getStartTs());
      } else if (context.getStartTs() != src.getStartTs()) {
        throw new DgraphException("startTs mismatch");
      }

      result.addAllKeys(src.getKeysList());

      this.context = result.build();
    }
  }
}
