package io.dgraph;

import org.junit.Test;

import java.io.IOException;

public class AclTest extends DgraphIntegrationTest {
    private static final String userid = "alice";
    private static final String userpassword = "simplepassword";
    private static final String dgraphEndpoint = TEST_HOSTNAME + "/" + TEST_PORT;
    private static final String grootPassword = "password";
    @Test
    public void testLogin() {

    }

    private void createAccountAndData() {
        dgraphClient.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());
    }

    private void resetUser() throws Exception {
        Process deleteUserCmd = new ProcessBuilder("dgraph", "acl", "userdel", "-d", dgraphEndpoint, "-u",
                userid, "-x", grootPassword).start();
        deleteUserCmd.waitFor();

        Process createUserCmd = new ProcessBuilder("dgraph", "acl", "useradd", "-d", dgraphEndpoint, "-u",
                userid,  "-p", userpassword, "-x", grootPassword).start();
        createUserCmd.waitFor();
        if (createUserCmd.exitValue() != 0) {
            throw new Exception("unable to create user");
        }
    }
}
