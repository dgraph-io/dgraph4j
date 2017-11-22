package io.dgraph;

import static junit.framework.TestCase.assertTrue;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphProto.Assigned;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AcctUpsertTest extends DgraphIntegrationTest {

  long lastStatus;
  AtomicInteger successCount = new AtomicInteger();
  AtomicInteger retryCount = new AtomicInteger();

  final String[] firsts = new String[] {"Paul", "Eric", "Jack", "John", "Martin"};
  final String[] lasts = new String[] {"Brown", "Smith", "Robinson", "Waters", "Taylor"};
  final int[] ages = new int[] {20, 25, 30, 35};

  ArrayList<Account> accounts = new ArrayList<>();

  private void setup() {
    // Setup data
    for (String first : firsts) {
      for (String last : lasts) {
        for (int age : ages) {
          Account acc = new Account();
          acc.first = first;
          acc.last = last;
          acc.age = age;
          accounts.add(acc);
        }
      }
    }

    String schema =
        "\n"
            + "   first:  string   @index(term) .\n"
            + "   last:   string   @index(hash) .\n"
            + "   age:    int      @index(int)  .\n"
            + "   when:   int                   .\n";
    Operation op = Operation.newBuilder().setSchema(schema).build();
    dgraphClient.alter(op);
  }

  private void tryUpsert(Account account) {
    Transaction txn = dgraphClient.newTransaction();
    String query =
        "\n"
            + "  {\n"
            + "   get(func: eq(first, \"%s\")) @filter(eq(last, \"%s\") AND eq(age, %d)) {\n"
            + "    uid: _uid_\n"
            + "   }\n"
            + "  }\n";
    query = String.format(query, account.first, account.last, account.age);
    try {
      Response resp = txn.query(query);
      Gson gson = new Gson();
      Decode1 decode1 = gson.fromJson(resp.getJson().toStringUtf8(), Decode1.class);
      assertTrue(decode1.get.size() <= 1);
      String uid;
      if (decode1.get.size() == 1) {
        uid = decode1.get.get(0).uid;
      } else {
        String nqs =
            "\n"
                + "   _:acct <first> \"%s\" .\n"
                + "   _:acct <last>  \"%s\" .\n"
                + "   _:acct <age>   \"%d\"^^<xs:int> .";
        nqs = String.format(nqs, account.first, account.last, account.age);
        Mutation mu = Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(nqs)).build();
        Assigned assigned = txn.mutate(mu);
        uid = assigned.getUidsOrThrow("acct");
      }

      String nq = String.format("<%s> <when> \"%d\"^^<xs:int> .", uid, System.nanoTime());
      Mutation mu = Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(nq)).build();
      txn.mutate(mu);
      txn.commit();
    } finally {
      txn.discard();
    }
  }

  private void upsert(Account account) {
    while (true) {
      long elapsed = new Date().getTime() - lastStatus;
      if (elapsed > 100) {
        System.out.printf("Success: %d Retries:%d\n", successCount.get(), retryCount.get());
        lastStatus = System.currentTimeMillis();
      }

      try {
        tryUpsert(account);
        successCount.addAndGet(1);
        return;
      } catch (RuntimeException ex) {
        // TODO check for conflict
        retryCount.addAndGet(1);
      }
    }
  }

  private void doUpserts() throws InterruptedException {
    ExecutorService ex = Executors.newFixedThreadPool(5);
    accounts.forEach(
        (account) -> {
          for (int i = 0; i < 5; i++) {
            ex.execute(() -> upsert(account));
          }
        });
    ex.shutdown();
    ex.awaitTermination(5, TimeUnit.MINUTES);
  }

  private void checkIntegrity() {
    String q =
        "{\n"
            + "   all(func: anyofterms(first, \"%s\")) {\n"
            + "    first\n"
            + "    last\n"
            + "    age\n"
            + "   }\n"
            + "}";
    q = String.format(q, String.join(" ", firsts));
    Response resp = dgraphClient.newTransaction().query(q);
    Gson gson = new Gson();
    Decode2 decode2 = gson.fromJson(resp.getJson().toStringUtf8(), Decode2.class);
    Set<String> accountSet = new HashSet<>();
    decode2.all.forEach(
        (record) -> {
          assertTrue(record.first != "");
          assertTrue(record.last != "");
          assertTrue(record.age != 0);
          String entry = String.format("%s_%s_%d", record.first, record.last, record.age);
          accountSet.add(entry);
        });
    assertTrue(accountSet.size() == accounts.size());
    accounts.forEach(
        (account) -> {
          String entry = String.format("%s_%s_%d", account.first, account.last, account.age);
          assertTrue(accountSet.contains(entry));
        });
  }

  @Test
  public void testAcctUpsert() throws InterruptedException {
    setup();
    doUpserts();
    checkIntegrity();
  }

  static class Account {
    String first;
    String last;
    int age;
  }

  static class Decode1 {
    ArrayList<Uids> get;

    static class Uids {
      String uid;
    }
  }

  static class Decode2 {
    List<Entry> all;

    static class Entry {
      String first;
      String last;
      int age;
    }
  }
}
