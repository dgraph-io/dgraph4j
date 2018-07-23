package io.dgraph;

import static java.util.Arrays.asList;

import io.dgraph.DgraphProto.LinRead;
import io.dgraph.DgraphProto.LinRead.Sequencing;
import io.dgraph.DgraphProto.Payload;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

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

  private final List<DgraphGrpc.DgraphStub> stubs;

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
  }

  private LinRead linRead = LinRead.newBuilder().build();

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
    return stubs.get(index);
  }

  public synchronized void mergeLinReads(LinRead fresh) {
    this.linRead = Helpers.mergeLinReads(linRead, fresh);
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
   * @param sequencing - the Sequencing strategy to be used
   * @return a new AsyncTransaction object.
   */
  public AsyncTransaction newTransaction(Sequencing sequencing) {
    return new AsyncTransaction(linRead, this::anyClient, this::mergeLinReads, sequencing);
  }

  public AsyncTransaction newTransaction() {
    return newTransaction(Sequencing.CLIENT_SIDE);
  }
}
