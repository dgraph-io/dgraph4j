/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Value;
import org.testng.annotations.Test;

public class ExceptionTest extends DgraphIntegrationTest {
  private NQuad quad =
      NQuad.newBuilder()
          .setSubject("0x01")
          .setPredicate("name")
          .setObjectValue(Value.newBuilder().setStrVal("Dgraph").build())
          .build();
  private Mutation mu = Mutation.newBuilder().addSet(quad).build();

  @Test(expectedExceptions = TxnConflictException.class)
  public void testConflictException() {
    Transaction txn1 = dgraphClient.newTransaction();
    Transaction txn2 = dgraphClient.newTransaction();
    txn1.mutate(mu);
    txn2.mutate(mu);
    txn1.commit();
    txn2.commit();
  }

  @Test(expectedExceptions = TxnFinishedException.class)
  public void testFinishedException() {
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(mu);
    txn.commit();
    txn.commit();
  }

  @Test(expectedExceptions = TxnReadOnlyException.class)
  public void testReadOnlyException() {
    Transaction txn = dgraphClient.newReadOnlyTransaction();
    txn.mutate(mu);
  }
}
