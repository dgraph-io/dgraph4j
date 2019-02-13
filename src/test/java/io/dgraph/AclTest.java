package io.dgraph;

import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.IOException;
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
  private static final String PREDICATE_TO_WRITE = "predicate_to_write";
  private static final String PREDICATE_TO_ALTER = "predicate_to_alter";
  private static final String QUERY_ATTR = "name";

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
    // initially all the operations should succeed when there are no rules
    // defined on the predicates (the fail open approach)
    queryPredicateWithUserAccount(false);
    mutatePredicateWithUserAccount(false);
    alterPredicateWithUserAccount(false);
  }

  private void createAccountAndData() throws Exception {
    resetUser();
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(
        DgraphProto.Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8("_:a <" + PREDICATE_TO_READ + "> \"SF\" ."))
            .build());
  }

  private void createGroupAndAcls(String group, boolean addUserToGroup)
      throws IOException, InterruptedException {
    // create a new group
    checkCmd(
        "unable to create the group " + group,
        "dgraph",
        "acl",
        "groupadd",
        "-d",
        DGRPAH_ENDPOINT,
        "-g",
        group,
        "-x",
        GROOT_PASSWORD);

    if (addUserToGroup) {
      checkCmd(
          "unable to add user " + USER_ID + " to the group " + group,
          "dgraph",
          "acl",
          "usermod",
          "-d",
          DGRPAH_ENDPOINT,
          "-u",
          USER_ID,
          "-g",
          group,
          "-x",
          GROOT_PASSWORD);
    }

    // add READ permission on the predicate_to_read to the group
    checkCmd(
        "unable to add READ permission on " + PREDICATE_TO_READ + " to the group " + group,
        "dgraph",
        "acl",
        "chmod",
        "-d",
        DGRPAH_ENDPOINT,
        "-g",
        group,
        "-p",
        PREDICATE_TO_READ,
        "-P",
        "4",
        "-x",
        GROOT_PASSWORD);

    // also add READ permission on the attribute queryAttr, which is used inside the query block
    checkCmd(
        "unable to add READ permission on " + QUERY_ATTR + " to the group " + group,
        "dgraph",
        "acl",
        "chmod",
        "-d",
        DGRPAH_ENDPOINT,
        "-g",
        group,
        "-p",
        QUERY_ATTR,
        "-P",
        "4",
        "-x",
        GROOT_PASSWORD);

    checkCmd(
        "unable to add WRITE permission on " + PREDICATE_TO_WRITE + " to the group " + group,
        "dgraph",
        "acl",
        "chmod",
        "-d",
        DGRPAH_ENDPOINT,
        "-g",
        group,
        "-p",
        PREDICATE_TO_WRITE,
        "-P",
        "2",
        "-x",
        GROOT_PASSWORD);
    checkCmd(
        "unable to add WRITE permission on " + PREDICATE_TO_ALTER + " to the group " + group,
        "dgraph",
        "acl",
        "chmod",
        "-d",
        DGRPAH_ENDPOINT,
        "-g",
        group,
        "-p",
        PREDICATE_TO_ALTER,
        "-P",
        "2",
        "-x",
        GROOT_PASSWORD);
  }

  private void checkCmd(String failureMsg, String... args)
      throws IOException, InterruptedException {
    Process cmd = new ProcessBuilder(args).start();
    cmd.waitFor();
    if (cmd.exitValue() != 0) {
      fail(failureMsg);
    }
  }

  private void queryPredicateWithUserAccount(boolean shouldFail) {
    verifyOperation(
        shouldFail,
        "query",
        () -> {
          String query =
              String.format(
                  "	{" + "q(func: eq(%s, \"SF\")) {" + "%s" + "}}", PREDICATE_TO_READ, QUERY_ATTR);
          Transaction txn = dgraphClient.newTransaction();
          txn.query(query);
        });
  }

  private void mutatePredicateWithUserAccount(boolean shouldFail) {
    verifyOperation(
        shouldFail,
        "mutation",
        () -> {
          Transaction txn = dgraphClient.newTransaction();
          txn.mutate(
              DgraphProto.Mutation.newBuilder()
                  .setCommitNow(true)
                  .setSetNquads(
                      ByteString.copyFromUtf8(
                          String.format("_:a <%s> \"string\" .", PREDICATE_TO_WRITE)))
                  .build());
        });
  }

  private void alterPredicateWithUserAccount(boolean shouldFail) {
    verifyOperation(
        shouldFail,
        "alter",
        () -> {
          dgraphClient.alter(
              DgraphProto.Operation.newBuilder()
                  .setSchema(String.format("%s: int .", PREDICATE_TO_ALTER))
                  .build());
        });
  }

  private void verifyOperation(boolean shouldFail, String operation, Runnable runnable) {
    boolean failed = false;
    try {
      runnable.run();
    } catch (io.grpc.StatusRuntimeException e) {
      e.printStackTrace();
      failed = true;
    }
    if (shouldFail && !failed) {
      fail("the " + operation + " should have failed");
    } else if (!shouldFail && failed) {
      fail("the " + operation + " should have succeed");
    }
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
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
      throw new Exception("unable to create user");
    }
  }
}
