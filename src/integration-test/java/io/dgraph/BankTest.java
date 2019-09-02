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

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class BankTest extends DgraphIntegrationTest {

  final ArrayList<String> uids = new ArrayList<>();
  final AtomicInteger runs = new AtomicInteger();
  final AtomicInteger aborts = new AtomicInteger();

  private void createAccounts() {
    String schema = "bal: int .";
    dgraphClient.alter(Operation.newBuilder().setSchema(schema).build());
    List<Account> accounts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Account acc = new Account();
      acc.bal = 100;
      accounts.add(acc);
    }
    Gson gson = new Gson();
    logger.debug(gson.toJson(accounts));
    Transaction txn = dgraphClient.newTransaction();
    Mutation mu =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(gson.toJson(accounts))).build();
    Response response = txn.mutate(mu);
    txn.commit();

    response.getUidsMap().forEach((key, uid) -> uids.add(uid));
  }

  private void runTotal() {
    String q =
        " {\n"
            + "   var(func: uid(%s)) {\n"
            + "    b as bal\n"
            + "   }\n"
            + "   total() {\n"
            + "    bal: sum(val(b))\n"
            + "   }\n"
            + "  }";
    q = String.format(q, String.join(",", uids));
    Transaction txn = dgraphClient.newTransaction();
    Response resp = txn.query(q);
    logger.debug("\nresponse json: {}\n", resp.getJson().toStringUtf8());
    logger.debug("Runs: {}. Aborts: {}\n", runs.get(), aborts.get());
  }

  private void runTotalInLoop() {
    while (true) {
      runTotal();
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // logger.debug(("runTotalInLoop interrupted"));
      }
    }
  }

  private void runTxn() {
    String from, to;
    Random rand = new Random();
    Gson gson = new Gson();
    while (true) {
      from = uids.get(rand.nextInt(uids.size()));
      to = uids.get(rand.nextInt(uids.size()));
      if (from != to) {
        break;
      }
    }

    Transaction txn = dgraphClient.newTransaction();
    try {
      String fq = String.format("{both(func: uid(%s, %s)) { uid, bal }}", from, to);
      Response resp = txn.query(fq);
      Accounts accounts = gson.fromJson(resp.getJson().toStringUtf8(), Accounts.class);
      if (accounts.both.size() != 2) {
        throw new RuntimeException("Unable to find both accounts");
      }

      accounts.both.get(0).bal += 5;
      accounts.both.get(1).bal -= 5;

      Mutation mu =
          Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(gson.toJson(accounts))).build();
      txn.mutate(mu);
      txn.commit();
    } finally {
      txn.discard();
    }
  }

  private void txnLoop() {
    while (true) {
      try {
        runTxn();
        final int r = runs.addAndGet(1);
        if (r > 1000) {
          return;
        }
      } catch (TxnConflictException e) {
        // logger.debug(e.getMessage());
        aborts.addAndGet(1);
      }
    }
  }

  @Test
  public void testBank() throws Exception {
    createAccounts();
    logger.debug(Arrays.toString(uids.toArray()));

    ExecutorService totalEx = Executors.newSingleThreadExecutor();
    totalEx.execute(() -> runTotalInLoop());
    totalEx.shutdown();

    ExecutorService txnEx = Executors.newCachedThreadPool();
    for (int i = 0; i < 10; i++) {
      txnEx.execute(() -> txnLoop());
    }
    txnEx.shutdown();

    if (!txnEx.awaitTermination(5, TimeUnit.MINUTES)) {
      logger.debug("Timeout elapsed");
    }
    totalEx.awaitTermination(5, TimeUnit.SECONDS);
  }

  static class Account {
    String uid;
    int bal;
  }

  static class Accounts {
    List<Account> both;
  }
}
