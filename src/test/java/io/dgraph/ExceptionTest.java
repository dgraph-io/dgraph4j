package io.dgraph;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Response;
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

  @Test
  public void testVectorSupport() {
    String vect = "[1, 2, 3, 4, 5]";
    NQuad quad1 =
        NQuad.newBuilder()
            .setSubject("0x1000")
            .setPredicate("productV")
            .setObjectValue(
                Value.newBuilder().setVfloat32Val(ByteString.copyFromUtf8(vect)).build())
            .build();

    System.out.println("quad1: " + quad1.toString());
    Mutation mu1 = Mutation.newBuilder().addSet(quad1).build();
    Transaction txn = dgraphClient.newTransaction();
    Response response1 = txn.mutate(mu1);
    System.out.printf("response:---------------------------> " + response1.toString());
    try {
      txn.commit();
    } catch (Exception e) {
      System.out.printf("response------------exe: " + e.toString());
    }

    String query =
        "{\n" + "  q(func: has(productV)) {\n" + "    uid\n" + "    productV\n" + "  }\n" + "}";

    // Create a transaction
    Transaction txn1 = dgraphClient.newTransaction();
    try {
      // Run the query
      DgraphProto.Response response = txn1.query(query);

      // Print the response JSON
      System.out.println(response.getJson().toStringUtf8());
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }
}
