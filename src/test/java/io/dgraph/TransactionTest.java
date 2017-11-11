package io.dgraph;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionTest {

  @Test
  public void merge() {
    DgraphProto.LinRead dst = DgraphProto.LinRead.newBuilder()
      .putIds(1, 10L).putIds(2, 15L).putIds(3, 10L).build();

    DgraphProto.LinRead src = DgraphProto.LinRead.newBuilder()
      .putIds(2, 10L).putIds(3, 15L).putIds(4, 10L).build();

    LinReadContext linReadContext = new LinReadContext();
    linReadContext.setLinRead(dst);
    Transaction transaction = new Transaction(null, linReadContext);

    DgraphProto.LinRead result = transaction.mergeLinReads(dst, src);
    assertEquals(3, result.getIdsCount());
    assertEquals(10L, result.getIdsOrThrow(1));
    assertEquals(15L, result.getIdsOrThrow(2));
    assertEquals(15L, result.getIdsOrThrow(3));
  }
}