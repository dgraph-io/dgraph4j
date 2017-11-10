package io.dgraph;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class DgraphAsyncClient {
    private static final Logger logger = LoggerFactory.getLogger(DgraphAsyncClient.class);

    private final List<DgraphGrpc.DgraphFutureStub> clients;
    private final LinReadContext linRead;

    public DgraphAsyncClient(List<DgraphGrpc.DgraphFutureStub> clients) {
      this.clients = ImmutableList.copyOf(clients);
      linRead = new LinReadContext();
    }

    public Transaction newTransaction() {
      return new Transaction(this::anyClient, linRead);
    }

    public void alter(DgraphProto.Operation op) {
      final DgraphGrpc.DgraphFutureStub client = anyClient();
      client.alter(op);
    }

    private DgraphGrpc.DgraphFutureStub anyClient() {
      Random rand = new Random();
      return clients.get(rand.nextInt(clients.size()));
    }
}
