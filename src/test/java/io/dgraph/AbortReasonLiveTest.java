/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.TxnConflictException.AbortReason;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Live cross-language end-to-end proof for the transaction-abort reason. Unlike {@link
 * AbortReasonTest}, which feeds synthetic gRPC statuses into the parser, this test drives a real
 * (locally patched) Dgraph server: it forces a genuine write-write conflict between two
 * transactions and asserts that the abort propagates all the way to {@link
 * TxnConflictException#getReason()} as {@link AbortReason#CONFLICT}. This closes the loop the unit
 * tests cannot — proving the server actually emits the categorized reason on the wire and the
 * client parses it.
 *
 * <p>Run against a non-ACL alpha listening on localhost:9180 (e.g. {@code dgraph alpha -o 100}).
 * It is intentionally standalone (does not extend {@link DgraphIntegrationTest}) so it needs only a
 * single alpha and no ACL login. Excluded from the default unit run unless selected explicitly via
 * {@code --tests io.dgraph.AbortReasonLiveTest}.
 */
public class AbortReasonLiveTest {
  private static final String HOST = "localhost";
  private static final int PORT = 9180;

  private static ManagedChannel channel;
  private static DgraphClient client;

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

    // Second commit must abort. The reason must reach the client end-to-end: a
    // TxnConflictException whose parsed reason is CONFLICT, still retryable, with the
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
}
