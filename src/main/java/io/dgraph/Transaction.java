package io.dgraph;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static net.javacrumbs.futureconverter.java8guava.FutureConverter.toCompletableFuture;

public class Transaction {
  private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

  private final Supplier<DgraphGrpc.DgraphFutureStub> stubSupplier;
  private final LinReadContext linReadContext;
  private DgraphProto.TxnContext context;
  private boolean finished;
  private boolean mutated;

  Transaction(Supplier<DgraphGrpc.DgraphFutureStub> stubSupplier, LinReadContext linReadContext) {
    this.stubSupplier = stubSupplier;
    this.linReadContext = linReadContext;
    context = DgraphProto.TxnContext.newBuilder().setLinRead(linReadContext.getLinRead()).build();
  }

  public DgraphProto.Response query(final String query, final Map<String, String> vars) {
    try {
      return queryAsync(query, vars).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new TxnException("Failed executing query in transaction started at " + context.getStartTs() + ": " +  e.getMessage(), e);
    }
  }

  public CompletableFuture<DgraphProto.Response> queryAsync(final String query, final Map<String, String> vars) {
    logger.debug("Starting query...");
    final DgraphProto.Request request = DgraphProto.Request.newBuilder()
      .setQuery(query)
      .putAllVars(vars)
      .setStartTs(context.getStartTs())
      .setLinRead(context.getLinRead())
      .build();
    final DgraphGrpc.DgraphFutureStub client = getClient();
    logger.debug("Sending request to Dgraph...");
    Stopwatch stopwatch = Stopwatch.createStarted();
    CompletableFuture<DgraphProto.Response> responseFuture = toCompletableFuture(client.query(request));
    responseFuture.thenAccept(response -> logger.debug("Received response from Dgraph, took {}", stopwatch.stop()));
    responseFuture.thenAccept(response -> mergeContext(response.getTxn()));
    return responseFuture;
  }

  public DgraphProto.Assigned mutate(DgraphProto.Mutation mutation) {
    try {
      return mutateAsync(mutation).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new TxnException("Failed mutation transaction started at " + context.getStartTs() + ": " + e.getMessage(), e);
    }
  }

  public CompletableFuture<DgraphProto.Assigned> mutateAsync(DgraphProto.Mutation mutation) {
    if (finished) {
      throw new TxnFinishedException();
    }

    DgraphProto.Mutation request = DgraphProto.Mutation.newBuilder(mutation).setStartTs(context.getStartTs()).build();

    final DgraphGrpc.DgraphFutureStub client = getClient();

    return toCompletableFuture(client.mutate(request)).whenComplete((assigned, throwable) -> {
      mutated = true;
      mergeContext(assigned.getContext());
      if (!assigned.getError().isEmpty()) {
        throw new DgraphException(assigned.getError());
      }
    });
  }

  public void commit() {
    try {
      commitAsync().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new TxnException("Failed committing transaction started at " + context.getStartTs() + ": " + e.getMessage(), e);
    }
  }

  public CompletableFuture<Void> commitAsync() {
    if (finished) {
      throw new TxnFinishedException();
    }

    finished = true;

    if (!mutated) {
      return CompletableFuture.completedFuture(null);
    }

    final DgraphGrpc.DgraphFutureStub client = getClient();
    return toCompletableFuture(client.commitOrAbort(context))
      .thenApply(txnContext -> {
        if (txnContext.getAborted()) {
          throw new TxnConflictException();
        }
        return null;
      });
  }

  public void discard() {
    try {
      discardAsync().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new TxnException("Failed discarding transaction started at " + context.getStartTs() + ": " + e.getMessage(), e);
    }
  }

  public CompletableFuture<Void> discardAsync() {
    if (finished) {
      return CompletableFuture.completedFuture(null);
    }
    finished = true;

    if (!mutated) {
      return CompletableFuture.completedFuture(null);
    }

    context = DgraphProto.TxnContext.newBuilder(context).setAborted(true).build();

    final DgraphGrpc.DgraphFutureStub client = getClient();
    return toCompletableFuture(client.commitOrAbort(context)).thenApply(txnContext -> null);
  }

  private void mergeContext(final DgraphProto.TxnContext txnContext) {
    DgraphProto.TxnContext.Builder result = DgraphProto.TxnContext.newBuilder(context);

    DgraphProto.LinRead lr = mergeLinReads(context.getLinRead(), txnContext.getLinRead());
    result.setLinRead(lr);

    lr = mergeLinReads(linReadContext.getLinRead(), lr);
    linReadContext.setLinRead(lr);

    if (context.getStartTs() == 0) {
      result.setStartTs(txnContext.getStartTs());
    } else if (context.getStartTs() != txnContext.getStartTs()) {
      throw new DgraphException("startTs mismatch (original " + context.getStartTs() + " conflicts with target " + txnContext.getStartTs() + ')');
    }

    result.addAllKeys(txnContext.getKeysList());

    context = result.build();
  }

  private DgraphGrpc.DgraphFutureStub getClient() {
    return stubSupplier.get();
  }

  private DgraphProto.LinRead mergeLinReads(DgraphProto.LinRead dst, DgraphProto.LinRead src) {
    DgraphProto.LinRead.Builder result = DgraphProto.LinRead.newBuilder(dst);
    for (Map.Entry<Integer, Long> entry : src.getIdsMap().entrySet()) {
      if (dst.containsIds(entry.getKey())
        && dst.getIdsOrThrow(entry.getKey()) >= entry.getValue()) {
        result.putIds(entry.getKey(), entry.getValue());
      }
    }
    return result.build();
  }

}
