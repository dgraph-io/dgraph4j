package io.dgraph;

import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the implementation of asynchronous Dgraph transaction. The asynchrony is backed-up by
 * underlying grpc implementation.
 *
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 * @author Michail Klimenkov
 */
public class AsyncTransaction implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncTransaction.class);

  // these can potentially be set from different threads executing the Stub callback
  private volatile TxnContext context;
  private volatile boolean mutated;
  private volatile boolean finished;

  private final LinRead.Sequencing sequencing;

  // provides
  private final Supplier<DgraphStub> stubSupplier;

  // LinReads are updated with every response received;
  // this is the way to convey that change
  private final Consumer<LinRead> linReadUpdateListener;

  AsyncTransaction(
      LinRead linRead,
      Supplier<DgraphStub> stubSupplier,
      Consumer<LinRead> linReadUpdateListener,
      LinRead.Sequencing sequencing) {
    this.context = TxnContext.newBuilder().setLinRead(linRead).build();

    this.sequencing = sequencing;
    this.stubSupplier = stubSupplier;
    this.linReadUpdateListener = linReadUpdateListener;
  }

  /**
   * sends a query to one of the connected dgraph instances. If no mutations need to be made in the
   * same transaction, it's convenient to chain the method: <code>
   * client.NewTransaction().queryWithVars(...)</code>.
   *
   * @param query Query in GraphQL+-
   * @param vars variables referred to in the queryWithVars.
   * @return a Response protocol buffer object.
   */
  public CompletableFuture<Response> queryWithVars(
      final String query, final Map<String, String> vars) {
    LOG.debug("Starting query...");

    LinRead.Builder lr = LinRead.newBuilder(context.getLinRead());
    lr.setSequencing(this.sequencing);
    final Request request =
        Request.newBuilder()
            .setQuery(query)
            .putAllVars(vars)
            .setStartTs(context.getStartTs())
            .setLinRead(lr.build())
            .build();

    LOG.debug("Sending request to Dgraph...");
    StreamObserverBridge<Response> bridge = new StreamObserverBridge<>();
    stubSupplier.get().query(request, bridge);

    return bridge
        .getDelegate()
        .thenApply(
            (Response response) -> {
              LOG.debug("Received response from Dgraph!");
              mergeContext(response.getTxn());
              return response;
            });
  }

  /**
   * Calls {@code Transcation#queryWithVars} with an empty vars map.
   *
   * @param query Query in GraphQL+-
   * @return a Response protocol buffer object
   */
  public CompletableFuture<Response> query(final String query) {
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
   *     committed. In this case, an explicit call to AsyncTransaction#commit doesn't need to
   *     subsequently be made.
   */
  public CompletableFuture<Assigned> mutate(Mutation mutation) {
    if (finished) {
      throw new TxnFinishedException();
    }

    Mutation request = Mutation.newBuilder(mutation).setStartTs(context.getStartTs()).build();

    StreamObserverBridge<Assigned> bridge = new StreamObserverBridge<>();
    stubSupplier.get().mutate(request, bridge);

    return bridge
        .getDelegate()
        .handle(
            (Assigned assigned, Throwable throwable) -> {
              if (throwable != null) {
                // IMPORTANT: the discard is asynchronous meaning that the remote
                // transaction may or may not be cancelled when this CompletionStage finishes.
                // All errors occurring during the discard are ignored.
                discard();
                throw launderException(throwable);
              } else {
                mutated = true;
                if (mutation.getCommitNow()) {
                  finished = true;
                }
                mergeContext(assigned.getContext());
                return assigned;
              }
            });
  }

  /**
   * Commits any mutations that have been made in the transaction. Once Commit has been called, the
   * lifespan of the transaction is complete.
   *
   * <p>Errors could be thrown for various reasons. Notably, a StatusRuntimeException could be
   * thrown if transactions that modify the same data are being run concurrently. It's up to the
   * user to decide if they wish to retry. In this case, the user should create a new transaction.
   *
   * @return CompletableFuture with Void result
   */
  public CompletableFuture<Void> commit() {
    if (finished) {
      CompletableFuture<Void> result = new CompletableFuture<>();
      result.completeExceptionally(new TxnFinishedException());
      return result;
    }

    finished = true;

    if (!mutated) {
      return CompletableFuture.completedFuture(null);
    }

    StreamObserverBridge<TxnContext> bridge = new StreamObserverBridge<>();
    stubSupplier.get().commitOrAbort(context, bridge);

    return bridge
        .getDelegate()
        .handle(
            (TxnContext txnContext, Throwable throwable) -> {
              if (throwable != null) {
                throw launderException(throwable);
              }
              return null;
            });
  }

  /**
   * Cleans up the resources associated with an uncommitted transaction that contains mutations. It
   * is a no-op on transactions that have already been committed or don't contain mutations.
   * Therefore it is safe (and recommended) to call it in a finally block.
   *
   * <p>In some cases, the transaction can't be discarded, e.g. the grpc connection is unavailable.
   * In these cases, the server will eventually do the transaction clean up.
   *
   * @return CompletableFuture with Void result
   */
  public CompletableFuture<Void> discard() {
    if (finished) {
      return CompletableFuture.completedFuture(null);
    }
    finished = true;

    if (!mutated) {
      return CompletableFuture.completedFuture(null);
    }

    context = TxnContext.newBuilder(context).setAborted(true).build();
    StreamObserverBridge<TxnContext> bridge = new StreamObserverBridge<>();
    stubSupplier.get().commitOrAbort(context, bridge);

    // we are providing void operation, so just nullify the result
    return bridge.getDelegate().thenApply((o) -> null);
  }

  private void mergeContext(final TxnContext src) {
    TxnContext.Builder builder = TxnContext.newBuilder(context);

    // update transaction context (LinReads) with response data
    LinRead lr = Helpers.mergeLinReads(this.context.getLinRead(), src.getLinRead());
    builder.setLinRead(lr);

    // update client LinReads with transaction LinReads
    linReadUpdateListener.accept(lr);

    if (context.getStartTs() == 0) {
      builder.setStartTs(src.getStartTs());
    } else if (context.getStartTs() != src.getStartTs()) {
      this.context = builder.build();
      throw new DgraphException("startTs mismatch");
    }

    builder.addAllKeys(src.getKeysList());

    this.context = builder.build();
  }

  private CompletionException launderException(Throwable ex) {
    if (ex instanceof CompletionStage) {
      Throwable cause = ex.getCause();

      if (cause instanceof StatusRuntimeException) {
        StatusRuntimeException ex1 = (StatusRuntimeException) ex;
        Status.Code code = ex1.getStatus().getCode();
        String desc = ex1.getStatus().getDescription();

        if (code.equals(Status.Code.ABORTED) || code.equals(Status.Code.FAILED_PRECONDITION)) {
          return new CompletionException(new TxnConflictException(desc));
        }
      }
      return (CompletionException) ex;
    }

    return new CompletionException(ex);
  }

  @Override
  public void close() {
    discard().join();
  }
}
