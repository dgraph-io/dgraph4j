package io.dgraph.example;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.Transaction;
import io.dgraph.TxnConflictException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;

public class MultiThreadedMutationLauncher {
  private static final String HOST = "localhost";
  private static final int PORT = 9080;
  private DgraphClient dgraphClient = null;

  public static void main(String[] args) {
    new MultiThreadedMutationLauncher().doProcess();
  }

  /*
   * Initialize Dgraph client in the constructor
   */
  public MultiThreadedMutationLauncher() {
    // initialize client
    ManagedChannel channel1 = ManagedChannelBuilder.forAddress(HOST, PORT).usePlaintext().build();
    DgraphStub stub1 = DgraphGrpc.newStub(channel1);
    dgraphClient = new DgraphClient(stub1);
  }

  /*
   * Sequence of processing steps in this example
   */
  private void doProcess() {
    // drops schema and data
    dropAll();
    // create a new schema
    createSchema();
    // initialize "Alice" with a clickCount of 1
    doSetupTransaction();

    // fire concurrent mutations
    doQueryAndMutation();
  }

  private void doQueryAndMutation() {
    // collect mutations
    List<MultiThreadedMutation> mutations = new ArrayList<MultiThreadedMutation>();
    for (int i = 0; i < 2; i++) {
      MultiThreadedMutation mtMutation = new MultiThreadedMutation(dgraphClient);
      mutations.add(mtMutation);
    }

    // launch threads
    for (MultiThreadedMutation mutation : mutations) {
      Thread t = new Thread(mutation);
      t.start();
    }
  }

  /*
   * Initialize a user "Alice" and "clickCount" attribute
   */
  private void doSetupTransaction() {
    Transaction txn = dgraphClient.newTransaction();
    Gson gson = new Gson();
    try {
      // Create Alice with a clickCount of 1
      Person personAlice = new Person();
      personAlice.name = "Alice";
      personAlice.clickCount = 1;

      String json = gson.toJson(personAlice);
      Mutation mu =
          Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json.toString())).build();
      txn.mutate(mu);
      txn.commit();

    } catch (TxnConflictException ex) {
      System.out.println(ex);
    } finally {
      txn.discard();
    }
  }

  /*
   * The schema for this example
   */
  private void createSchema() {
    String schema = "name: string @index(exact) .\n " + "clickCount: int  .\n";

    Operation operation = Operation.newBuilder().setSchema(schema).setRunInBackground(true).build();
    dgraphClient.alter(operation);
  }
  /*
   * Drop schema and data in the Dgraph instance
   */
  private void dropAll() {
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
    System.out.println("existing schema dropped");
  }
}
