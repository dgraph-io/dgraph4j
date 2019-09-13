package io.dgraph;

import static org.testng.Assert.fail;

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

  @Test
  public void testConflictException() {
    Transaction txn1 = dgraphClient.newTransaction();
    Transaction txn2 = dgraphClient.newTransaction();
    txn1.mutate(mu);
    txn2.mutate(mu);

    txn1.commit();
    try {
      txn2.commit();
      fail("should not reach here");
    } catch (TxnConflictException ignored) {
    }
  }

  @Test
  public void testFinishedException() {
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(mu);
    txn.commit();
    try {
      txn.commit();
      fail("should not reach here");
    } catch (TxnFinishedException ignored) {
    }
  }

  @Test
  public void testReadOnlyException() {
    Transaction txn = dgraphClient.newReadOnlyTransaction();
    try {
      txn.mutate(mu);
      fail("should not reach here");
    } catch (TxnReadOnlyException ignored) {
    }
  }
}
