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
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
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

  private int deadlineSecs;

  private LinRead linRead;

  final ReentrantLock lrLck = new ReentrantLock();

  LinRead getLinRead() {
    lrLck.lock();
    LinRead lr = LinRead.newBuilder(linRead).build();
    lrLck.unlock();
    return lr;
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
   * Creates a new Dgraph for interacting with a Dgraph store, with the the specified deadline.
   *
   * <p>A single client is thread safe.
   *
   * @param clients One or more synchronous grpc clients. Can contain connections to multiple
   *     servers in a cluster.
   * @param deadlineSecs Deadline specified in secs, after which the client will timeout.
   */
  public DgraphClient(List<DgraphGrpc.DgraphBlockingStub> clients, int deadlineSecs) {
    this(clients);
    this.deadlineSecs = deadlineSecs;
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

  /**
   * Sets the edges corresponding to predicates on the node with the given uid for deletion. This
   * function returns a new Mutation object with the edges set. It is the caller's responsibility to
   * run the mutation by calling {@code Transaction#mutate}.
   *
   * @param mu Mutation to add edges to
   * @param uid uid of the node
   * @param predicates predicates of the edges to remove
   * @return a new Mutation object with the edges set
   */
  public static Mutation deleteEdges(Mutation mu, String uid, String... predicates) {
    Mutation.Builder b = Mutation.newBuilder(mu);
    for (String predicate : predicates) {
      b.addDel(
          NQuad.newBuilder()
              .setSubject(uid)
              .setPredicate(predicate)
              .setObjectValue(Value.newBuilder().setDefaultVal("_STAR_ALL").build())
              .build());
    }
    return b.build();
  }

  private DgraphGrpc.DgraphBlockingStub anyClient() {
    Random rand = new Random();

    DgraphGrpc.DgraphBlockingStub client = clients.get(rand.nextInt(clients.size()));

    if (deadlineSecs > 0) {
      return client.withDeadlineAfter(deadlineSecs, TimeUnit.SECONDS);
    }

    return client;
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

  public class Transaction implements AutoCloseable {
    TxnContext context;
    boolean finished;
    boolean mutated;
    LinRead.Sequencing sequencing;

    Transaction() {
      context = TxnContext.newBuilder().setLinRead(DgraphClient.this.getLinRead()).build();
    }

		/**
		 * Sets the sequencing for this transaction. By default client_side_sequencing is used
		 *
		 * @param sequencing Sequencing Mode (ClIENT_SIDE or SERVER_SIDE).
		 */
    public void setSequencing(LinRead.Sequencing sequencing) {
      this.sequencing = sequencing;
    }

    /**
     * sends a query to one of the connected dgraph instances. If no mutations need to be made in
     * the same transaction, it's convenient to chain the method: <code>
     * client.NewTransaction().queryWithVars(...)</code>.
     *
     * @param query Query in GraphQL+-
     * @param vars variables referred to in the queryWithVars.
     * @return a Response protocol buffer object.
     */
    public Response queryWithVars(final String query, final Map<String, String> vars) {
      logger.debug("Starting query...");
      LinRead.Builder lr = LinRead.newBuilder(context.getLinRead());
      lr.setSequencing(this.sequencing);
      final Request request =
          Request.newBuilder()
              .setQuery(query)
              .putAllVars(vars)
              .setStartTs(context.getStartTs())
              .setLinRead(lr.build())
              .build();
      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      logger.debug("Sending request to Dgraph...");
      final Response response = client.query(request);
      logger.debug("Received response from Dgraph!");
      mergeContext(response.getTxn());
      return response;
    }

    /**
     * Calls {@code Transcation#queryWithVars} with an empty vars map.
     *
     * @param query Query in GraphQL+-
     * @return a Response protocol buffer object
     */
    public Response query(final String query) {
      return queryWithVars(query, Collections.emptyMap());
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
      Assigned ag;
      try {
        ag = client.mutate(request);
        mutated = true;
        if (mutation.getCommitNow()) {
          finished = true;
        }
        mergeContext(ag.getContext());
        return ag;
      } catch (RuntimeException ex) {
        try {
          // Since a mutation error occurred, the txn should no longer be used
          // (some mutations could have applied but not others, but we don't know
          // which ones).  Discarding the transaction enforces that the user
          // cannot use the txn further.
          discard();
        } catch (RuntimeException ex1) {
          // Ignore error - user should see the original error.
        }
        checkAndThrowException(ex);
      }
      return null;
    }

    /**
     * Commits any mutations that have been made in the transaction. Once Commit has been called,
     * the lifespan of the transaction is complete.
     *
     * <p>Errors could be thrown for various reasons. Notably, a StatusRuntimeException could be
     * thrown if transactions that modify the same data are being run concurrently. It's up to the
     * user to decide if they wish to retry. In this case, the user should create a new transaction.
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
      try {
        client.commitOrAbort(context);
      } catch (RuntimeException ex) {
        checkAndThrowException(ex);
      }
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

      lrLck.lock();
      lr = mergeLinReads(DgraphClient.this.linRead, lr);
      DgraphClient.this.linRead = lr;
      lrLck.unlock();

      if (context.getStartTs() == 0) {
        result.setStartTs(src.getStartTs());
      } else if (context.getStartTs() != src.getStartTs()) {
        this.context = result.build();
        throw new DgraphException("startTs mismatch");
      }

      result.addAllKeys(src.getKeysList());

      this.context = result.build();
    }

    // Check if Txn has been aborted and throw a TxnConflictException,
    // otherwise throw the original exception.
    private void checkAndThrowException(RuntimeException ex) {
      if (ex instanceof StatusRuntimeException) {
        StatusRuntimeException ex1 = (StatusRuntimeException) ex;
        Code code = ex1.getStatus().getCode();
        String desc = ex1.getStatus().getDescription();
        if (code.equals(Code.ABORTED) || code.equals(Code.FAILED_PRECONDITION)) {
          throw new TxnConflictException(desc);
        }
      }
      throw ex;
    }

    @Override
    public void close() {
      discard();
    }
  }
}
