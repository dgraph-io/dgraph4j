package io.dgraph;

import com.codesnippets4all.json.parsers.JSONParser;
import com.codesnippets4all.json.parsers.JsonParserFactory;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AclTest {
  private static final String USER_ID = "alice";
  private static final String USER_PASSWORD = "simplepassword";
  private static final String GROOT_PASSWORD = "password";
  private static final String PREDICATE_TO_READ = "predicate_to_read";
  private static final String QUERY_ATTR = "name";
  private static final JSONParser JSON_PARSER = JsonParserFactory.getInstance().newJsonParser();

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;
  private static final String DGRPAH_ENDPOINT = TEST_HOSTNAME + ":" + TEST_PORT;
  private static ManagedChannel channel;
  protected static DgraphClient dgraphClient;

  @BeforeClass
  public static void beforeClass() {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
    dgraphClient = new DgraphClient(stub);
    dgraphClient.login("groot", GROOT_PASSWORD);
    dgraphClient.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());
    dgraphClient.alter(
        DgraphProto.Operation.newBuilder()
            .setSchema(PREDICATE_TO_READ + ": string @index(exact) .")
            .build());
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void testLogin() throws Exception {
    createAccountAndData();
    queryPredicateWithUserAccount(false);
  }

  private void createAccountAndData() throws Exception {
    resetUser();
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(
        DgraphProto.Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8("_:a <" + PREDICATE_TO_READ + "> \"SF\" ."))
            .build());
  }

  private void queryPredicateWithUserAccount(boolean shouldFail) {
    String query =
        String.format(
            "	{" + "q(func: eq(%s, \"SF\")) {" + "%s" + "}}", PREDICATE_TO_READ, QUERY_ATTR);
    Transaction txn = dgraphClient.newTransaction();
    DgraphProto.Response resp = txn.query(query);

    System.out.println("response:\n" + resp.getJson());
  }

  private void resetUser() throws Exception {
    Process deleteUserCmd =
        new ProcessBuilder(
                "dgraph",
                "acl",
                "userdel",
                "-d",
                DGRPAH_ENDPOINT,
                "-u",
                USER_ID,
                "-x",
                GROOT_PASSWORD)
            .start();
    deleteUserCmd.waitFor();

    Process createUserCmd =
        new ProcessBuilder(
                "dgraph",
                "acl",
                "useradd",
                "-d",
                DGRPAH_ENDPOINT,
                "-u",
                USER_ID,
                "-p",
                USER_PASSWORD,
                "-x",
                GROOT_PASSWORD)
            .redirectErrorStream(true)
            .start();
    createUserCmd.waitFor();
    if (createUserCmd.exitValue() != 0) {
      // print out the output from the command
      InputStream inputStream = createUserCmd.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
      throw new Exception("unable to create user");
    }
  }
}
