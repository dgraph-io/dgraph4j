package io.dgraph;

import static org.testng.Assert.*;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AclTest extends DgraphIntegrationTest {
  private static final String USER_ID = "alice";
  private static final String USER_PASSWORD = "simplepassword";
  private static final String PREDICATE_TO_READ = "predicate_to_read";
  private static final String PREDICATE_TO_WRITE = "predicate_to_write";
  private static final String PREDICATE_TO_ALTER = "predicate_to_alter";
  private static final String QUERY_ATTR = "name";
  private static final String UNUSED_GROUP = "unusedGroup";
  private static final String DEV_GROUP = "dev";

  @BeforeClass
  public void setSchema() {
    dgraphClient.alter(
        DgraphProto.Operation.newBuilder()
            .setSchema(PREDICATE_TO_READ + ": string @index(exact) .")
            .build());
  }

  @Test(groups = {"acl"})
  public void testAuthorization() throws Exception {
    createAccountAndData();

    // initially all the operations should fail
    dgraphClient.login(USER_ID, USER_PASSWORD);
    queryPredicateWithUserAccount(true);
    mutatePredicateWithUserAccount(true);
    alterPredicateWithUserAccount(true);

    createGroupAndACLs(UNUSED_GROUP, false);
    System.out.println("Sleep for 6 seconds for acl caches to be refreshed");
    Thread.sleep(6 * 1000);

    // now all the operations should still fail
    queryPredicateWithUserAccount(true);
    mutatePredicateWithUserAccount(true);
    alterPredicateWithUserAccount(true);

    // create the dev group and add the user to it
    createGroupAndACLs(DEV_GROUP, true);
    System.out.println("Sleep for 6 seconds for acl caches to be refreshed");
    Thread.sleep(6 * 1000);

    // now the operations should succeed again through the dev group
    queryPredicateWithUserAccount(false);
    // sleep long enough (10s per the docker-compose.yml in this directory)
    // for the accessJwt to expire in order to test auto login through refresh jwt
    System.out.println("Sleep for 4 seconds for the accessJwt to expire");
    Thread.sleep(4 * 1000);
    mutatePredicateWithUserAccount(false);
    System.out.println("Sleep for 4 seconds for the accessJwt to expire");
    Thread.sleep(4 * 1000);
    alterPredicateWithUserAccount(false);

    // remove the user from dev group
    removeUserFromAllGroups();

    // now all operations should fail again
    Thread.sleep(6 * 1000);
    queryPredicateWithUserAccount(true);
    mutatePredicateWithUserAccount(true);
    alterPredicateWithUserAccount(true);
  }

  private void createAccountAndData() throws Exception {
    resetUser();
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(
        DgraphProto.Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8("_:a <" + PREDICATE_TO_READ + "> \"SF\" ."))
            .build());
  }

  private void createGroupAndACLs(String group, boolean addUserToGroup) throws Exception {

    // create a new group
    TestUtil.addGroup(group);

    if (addUserToGroup) {
      TestUtil.updateUser(USER_ID, group, true);
    }

    // add READ permission on the predicate_to_read to the group
    TestUtil.updateGroup(group, PREDICATE_TO_READ, 4);

    // also add READ permission on the attribute queryAttr, which is used inside the query block
    TestUtil.updateGroup(group, QUERY_ATTR, 4);

    TestUtil.updateGroup(group, PREDICATE_TO_WRITE, 2);

    TestUtil.updateGroup(group, PREDICATE_TO_ALTER, 1);
  }

  private void removeUserFromAllGroups() throws Exception {
    TestUtil.updateUser(USER_ID, DEV_GROUP, false);
  }

  private void queryPredicateWithUserAccount(boolean shouldFail) {
    String query =
        String.format(
            "	{" + "users(func: eq(%s, \"SF\")) {" + "%s" + "}}", PREDICATE_TO_READ, QUERY_ATTR);
    Transaction txn = dgraphClient.newTransaction();
    DgraphProto.Response response = txn.query(query);

    // Queries do not fail due to ACL because they just do not return
    // the predicates that the user do not have access to.
    if (shouldFail) {
      String result = response.getJson().toStringUtf8();
      assertEquals(result, "{}", "the operation should have failed");
    }
    txn.discard();
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
        () ->
            dgraphClient.alter(
                DgraphProto.Operation.newBuilder()
                    .setSchema(String.format("%s: int .", PREDICATE_TO_ALTER))
                    .build()));
  }

  /**
   * verifyOperations executes the runnable, and then checks whether the runable runs into any
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
      assertEquals(Status.Code.PERMISSION_DENIED, statusRuntimeException.getStatus().getCode());
      return;
    }

    assertFalse(shouldFail, "the " + operation + " should have failed");
  }

  private void resetUser() throws Exception {
    TestUtil.deleteUser(USER_ID);
    TestUtil.addUser(USER_ID, USER_PASSWORD);
  }

  private void checkCmd(String failureMsg, String... args)
      throws IOException, InterruptedException {
    Process cmd = new ProcessBuilder(args).redirectErrorStream(true).start();
    cmd.waitFor();
    if (cmd.exitValue() != 0) {
      BufferedReader br = new BufferedReader(new InputStreamReader(cmd.getInputStream()));
      fail(failureMsg + "\n" + br.lines().collect(Collectors.joining("\n")));
    }
  }
}
