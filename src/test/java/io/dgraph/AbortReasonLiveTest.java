/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.TxnConflictException.AbortReason;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Live cross-language end-to-end proof for the transaction-abort reason. Unlike {@link
 * AbortReasonTest}, which feeds synthetic gRPC statuses into the parser, this test drives a real
 * (locally patched) Dgraph server and asserts that each abort category propagates all the way to
 * {@link TxnConflictException#getReason()}. This closes the loop the unit tests cannot — proving the
 * server actually emits the categorized reason on the wire and the client parses it.
 *
 * <p>Configuration (system properties or environment variables):
 *
 * <ul>
 *   <li>{@code dgraph.test.host} / {@code TEST_HOSTNAME} — alpha host (default {@code localhost})
 *   <li>{@code dgraph.test.port} / {@code TEST_GRPC_PORT} — alpha gRPC port (default {@code 9180})
 *   <li>{@code dgraph.test.zeroHttp} / {@code TEST_ZERO_HTTP} — zero HTTP admin (e.g. {@code
 *       localhost:6180}); enables the predicate-move test (needs a multi-group cluster)
 *   <li>{@code dgraph.test.zeroRestartCmd} / {@code TEST_ZERO_RESTART_CMD} — shell command that
 *       restarts Zero; enables the stale-startts test
 * </ul>
 *
 * Tests whose infrastructure is not configured are skipped (not failed), so the file is safe in the
 * default run. Run explicitly with {@code --tests io.dgraph.AbortReasonLiveTest}.
 */
public class AbortReasonLiveTest {
  private static final String HOST = conf("dgraph.test.host", "TEST_HOSTNAME", "localhost");
  private static final int PORT =
      Integer.parseInt(conf("dgraph.test.port", "TEST_GRPC_PORT", "9180"));
  private static final String ZERO_HTTP = conf("dgraph.test.zeroHttp", "TEST_ZERO_HTTP", null);
  private static final String ZERO_RESTART_CMD =
      conf("dgraph.test.zeroRestartCmd", "TEST_ZERO_RESTART_CMD", null);

  private static ManagedChannel channel;
  private static DgraphClient client;

  private static String conf(String prop, String env, String dflt) {
    String v = System.getProperty(prop);
    if (v == null || v.isEmpty()) {
      v = System.getenv(env);
    }
    return (v == null || v.isEmpty()) ? dflt : v;
  }

  @BeforeClass
  public static void before() {
    channel = ManagedChannelBuilder.forAddress(HOST, PORT).usePlaintext().build();
    client = new DgraphClient(DgraphGrpc.newStub(channel));
    client.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @AfterClass
  public static void after() throws InterruptedException {
    if (channel != null) {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void liveConflictReportsConflictReason() {
    // txn1 creates a node with a name.
    Transaction txn1 = client.newTransaction();
    Mutation mu1 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8("{\"name\": \"Manish\"}")).build();
    Response assigned = txn1.mutate(mu1);
    assertEquals(assigned.getUidsMap().size(), 1, "expected exactly one assigned uid");
    String uid = assigned.getUidsMap().values().iterator().next();

    // txn2 writes the same predicate on the same uid -> conflicts.
    Transaction txn2 = client.newTransaction();
    Mutation mu2 =
        Mutation.newBuilder()
            .setSetJson(ByteString.copyFromUtf8("{\"uid\": \"" + uid + "\", \"name\": \"Manish\"}"))
            .build();
    txn2.mutate(mu2);

    // First commit wins; its commitTs is now greater than txn2's startTs.
    txn1.commit();

    // Second commit must abort with the CONFLICT category, still retryable, with the
    // full "conflict: ..." server message preserved.
    try {
      txn2.commit();
      fail("expected the conflicting second commit to throw TxnConflictException");
    } catch (TxnConflictException e) {
      assertEquals(
          e.getReason(),
          AbortReason.CONFLICT,
          "server-reported reason should parse to CONFLICT; full message: " + e.getMessage());
      assertTrue(e.isRetryable(), "conflict aborts are retryable");
      assertTrue(
          e.getMessage().contains("conflict:"),
          "full categorized server message should be preserved; got: " + e.getMessage());
    }
  }

  /**
   * A transaction's start ts becomes "stale" when it predates the current Zero leader's lease — i.e.
   * after a leader change. We force that by opening a transaction and then restarting Zero (via the
   * configured command): on restart Zero renews its lease and advances startTxnTs past every
   * previously-leased start ts, so committing the now-old txn aborts with STALE_STARTTS.
   */
  @Test
  public void liveStaleStartTsReportsStaleReason() throws Exception {
    if (ZERO_RESTART_CMD == null) {
      throw new SkipException(
          "set dgraph.test.zeroRestartCmd / TEST_ZERO_RESTART_CMD to restart Zero");
    }

    // Open a transaction so it gets a start ts that the restart will invalidate.
    Transaction txn = client.newTransaction();
    txn.mutate(
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8("{\"name\": \"Manish\"}")).build());

    // Restart Zero; sleeps give the leader time to re-establish (lease renewal, hence the
    // startTxnTs bump, runs on becoming leader).
    runShell(ZERO_RESTART_CMD);
    Thread.sleep(8000);

    try {
      txn.commit();
      fail("expected the stale commit to throw TxnConflictException");
    } catch (TxnConflictException e) {
      assertEquals(
          e.getReason(),
          AbortReason.STALE_STARTTS,
          "server-reported reason should parse to STALE_STARTTS; full message: " + e.getMessage());
      assertTrue(
          e.getMessage().contains("stale-startts:"),
          "full categorized server message should be preserved; got: " + e.getMessage());
    }
  }

  /**
   * Moving a predicate's tablet to another group rejects commits that mutated it on the old group.
   * We mutate "name" while its tablet is on the source group, move the tablet, then commit: the
   * commit's predicate keys reference the old group, so Zero's checkPreds rejects it with the
   * PREDICATE_MOVE category.
   */
  @Test
  public void livePredicateMoveReportsPredicateMoveReason() throws Exception {
    if (ZERO_HTTP == null) {
      throw new SkipException(
          "set dgraph.test.zeroHttp / TEST_ZERO_HTTP and run a multi-group cluster");
    }

    client.alter(Operation.newBuilder().setSchema("name: string @index(exact) .").build());

    // Seed so the "name" tablet exists and settles on some group.
    Transaction seed = client.newTransaction();
    seed.mutate(
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8("{\"name\": \"seed\"}")).build());
    seed.commit();
    Thread.sleep(1000);

    String src = groupOf("name");
    if (src == null || groupCount() < 2) {
      throw new SkipException("need a multi-group cluster serving predicate 'name'");
    }
    String dst = src.equals("1") ? "2" : "1";

    // Mutate "name" while it is on `src` (the txn's predicate keys reference `src`), don't commit.
    Transaction txn = client.newTransaction();
    txn.mutate(
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8("{\"name\": \"Manish\"}")).build());

    // Move the tablet and wait for the move to complete.
    httpGet("http://" + ZERO_HTTP + "/moveTablet?tablet=name&group=" + dst);
    long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline && !dst.equals(groupOf("name"))) {
      Thread.sleep(1000);
    }
    assertEquals(groupOf("name"), dst, "tablet move did not complete");

    try {
      txn.commit();
      fail("expected the post-move commit to throw TxnConflictException");
    } catch (TxnConflictException e) {
      assertEquals(
          e.getReason(),
          AbortReason.PREDICATE_MOVE,
          "server-reported reason should parse to PREDICATE_MOVE; full message: " + e.getMessage());
      assertTrue(
          e.getMessage().contains("predicate-move:"),
          "full categorized server message should be preserved; got: " + e.getMessage());
    }
  }

  // --- helpers ---

  @SuppressWarnings("unchecked")
  private static Map<String, Object> zeroState() throws Exception {
    String body = httpGet("http://" + ZERO_HTTP + "/state");
    return new Gson().fromJson(body, Map.class);
  }

  /** Returns the group id serving the given predicate (matching the namespaced tablet key). */
  @SuppressWarnings("unchecked")
  private static String groupOf(String pred) throws Exception {
    Map<String, Object> groups = (Map<String, Object>) zeroState().get("groups");
    if (groups == null) {
      return null;
    }
    for (Map.Entry<String, Object> e : groups.entrySet()) {
      Object tabletsObj = ((Map<String, Object>) e.getValue()).get("tablets");
      if (tabletsObj instanceof Map) {
        for (String tablet : ((Map<String, Object>) tabletsObj).keySet()) {
          if (tablet.equals(pred) || tablet.endsWith("-" + pred)) {
            return e.getKey();
          }
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static int groupCount() throws Exception {
    Map<String, Object> groups = (Map<String, Object>) zeroState().get("groups");
    return groups == null ? 0 : groups.size();
  }

  private static String httpGet(String urlStr) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
    conn.setRequestMethod("GET");
    try (BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = rd.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    } finally {
      conn.disconnect();
    }
  }

  private static void runShell(String cmd) throws Exception {
    Process p = new ProcessBuilder("bash", "-c", cmd).inheritIO().start();
    if (!p.waitFor(60, TimeUnit.SECONDS)) {
      p.destroyForcibly();
      throw new RuntimeException("zero restart command timed out: " + cmd);
    }
    if (p.exitValue() != 0) {
      throw new RuntimeException("zero restart command failed (" + p.exitValue() + "): " + cmd);
    }
  }
}
