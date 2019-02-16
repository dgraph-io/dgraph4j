package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AclTest {
  private static final String USER_ID = "alice";
  private static final String USER_PASSWORD = "simplepassword";
  private static final String GROOT_PASSWORD = "password";
  private static final String PREDICATE_TO_READ = "predicate_to_read";
  private static final String PREDICATE_TO_WRITE = "predicate_to_write";
  private static final String PREDICATE_TO_ALTER = "predicate_to_alter";
  private static final String QUERY_ATTR = "name";
  private static final String UNUSED_GROUP = "unusedGroup";
  private static final String DEV_GROUP = "dev";

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;
  private static final String DGRPAH_ENDPOINT = TEST_HOSTNAME + ":" + TEST_PORT;
  private static ManagedChannel channel;
  protected static DgraphClient dgraphClient;

  @BeforeClass
  public void beforeClass() throws IOException, InterruptedException {
    // start the cluster using the $GOPATH/src/github.com/dgraph-io/dgraph/ee/acl/docker-compose.yml
    TestUtils.checkCmd(
        "unable to start the cluster",
        "docker-compose",
        "-f",
        System.getenv("GOPATH") + "/src/github.com/dgraph-io/dgraph/ee/acl/docker-compose.yml",
        "up",
        "--force-recreate",
        "--remove-orphans",
        "--detach");
    System.out.println("Started the dgraph cluster. Sleeping for 10s for cluster to stabilize");
    // sleep for 10 seconds for the cluster to stablize
    Thread.sleep(10 * 1000);

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
  public void afterClass() throws InterruptedException, IOException {
    if (channel != null) {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    // tear down the cluster
    TestUtils.checkCmd(
        "unable to start the cluster",
        "docker-compose",
        "-f",
        System.getenv("GOPATH") + "/src/github.com/dgraph-io/dgraph/ee/acl/docker-compose.yml",
        "down");
  }

  @Test(groups = {"acl"})
  public void testAuthorization() throws Exception {
    createAccountAndData();
    // initially all the operations should succeed when there are no rules
    // defined on the predicates (the fail open approach)
    dgraphClient.login(USER_ID, USER_PASSWORD);
    queryPredicateWithUserAccount(false);
    mutatePredicateWithUserAccount(false);
    alterPredicateWithUserAccount(false);

    createGroupAndAcls(UNUSED_GROUP, false);
    System.out.println("Sleep for 35 seconds for acl caches to be refreshed");
    Thread.sleep(35 * 1000);

    // now all the operations should fail since there are rules defined on the unusedGroup
    queryPredicateWithUserAccount(true);
    mutatePredicateWithUserAccount(true);

    alterPredicateWithUserAccount(true);

    // create the dev group and add the user to it
    createGroupAndAcls(DEV_GROUP, true);
    System.out.println("Sleep for 35 seconds for acl caches to be refreshed");
    Thread.sleep(35 * 1000);

    // now the operations should succeed again through the dev group
    queryPredicateWithUserAccount(false);
    // sleep long enough (10s per the docker-compose.yml in this directory)
    // for the accessJwt to expire in order to test auto login through refresh jwt
    System.out.println("Sleep for 12 seconds for the accessJwt to expire");
    Thread.sleep(12 * 1000);
    mutatePredicateWithUserAccount(false);
    System.out.println("Sleep for 12 seconds for the accessJwt to expire");
    Thread.sleep(12 * 1000);
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
    TestUtils.checkCmd(
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
      TestUtils.checkCmd(
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
    TestUtils.checkCmd(
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
    TestUtils.checkCmd(
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

    TestUtils.checkCmd(
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
    TestUtils.checkCmd(
        "unable to add ALTER permission on " + PREDICATE_TO_ALTER + " to the group " + group,
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
        "1",
        "-x",
        GROOT_PASSWORD);
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

  /**
   * verifyOperitans executes the runnable, and then checks whether the runable runs into any
   * exception. If the shouldFail is true, and the runnable does not encounter any exception, this
   * method will fail the test. On the other hand, if the shouldFail is false, and the runnable
   * encounters an exception, this method will also fail the test.
   *
   * @param shouldFail whether the runnable should fail
   * @param operation the operation name of the runnable
   * @param runnable the runnable to be executed
   */
  private void verifyOperation(boolean shouldFail, String operation, Runnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeException e) {
      assertTrue(shouldFail, "the " + operation + " should have succeed");
      // if there is an exception, we assert that it must be caused by permission being denied
      Throwable cause = e;
      while (cause.getCause() != null && !(cause.getCause() instanceof StatusRuntimeException)) {
        cause = cause.getCause();
      }

      assertTrue(
          cause.getCause() != null && cause.getCause() instanceof io.grpc.StatusRuntimeException);
      StatusRuntimeException statusRuntimeException = (StatusRuntimeException) cause.getCause();
      e.printStackTrace();
      assertEquals(Status.Code.PERMISSION_DENIED, statusRuntimeException.getStatus().getCode());
      return;
    }

    assertFalse(shouldFail, "the " + operation + " should have failed");
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
