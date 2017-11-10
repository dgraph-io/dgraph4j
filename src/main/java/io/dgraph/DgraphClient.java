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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Implementation of a DgraphClient using grpc.
 *
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 * @version 0.0.2
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

  public DgraphClient(List<DgraphGrpc.DgraphBlockingStub> clients) {
    this.clients = clients;
    linRead = LinRead.getDefaultInstance();
  }

  public Transaction newTransaction() {
    return new Transaction();
  }

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

    public void commit() {
      if (finished) {
        throw new TxnFinishedException();
      }

      finished = true;

      if (!mutated) {
        return;
      }

      final DgraphGrpc.DgraphBlockingStub client = anyClient();
      final TxnContext ctx = client.commitOrAbort(context);
      if (ctx.getAborted()) {
        throw new TxnConflictException();
      }
    }

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
