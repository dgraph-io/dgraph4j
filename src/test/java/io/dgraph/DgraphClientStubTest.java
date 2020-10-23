package io.dgraph;

import org.testng.annotations.Test;

import java.net.MalformedURLException;

import static org.testng.Assert.fail;

public class DgraphClientStubTest {

    @Test
    public void testFromSlashEndpoint_ValidURL() {
        try {
            DgraphGrpc.DgraphStub stub =
                    DgraphClientStub.fromSlashEndpoint(
                            "https://your-slash" + "-instance.cloud.dgraph.io/graphql", "");
        } catch (MalformedURLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testFromSlashEndpoint_InValidURL() {
        try {
            DgraphGrpc.DgraphStub stub = DgraphClientStub.fromSlashEndpoint("https://a-bad-url", "");
            fail("Invalid Slash URL should not be accepted.");
        } catch (MalformedURLException e) {
        }
    }
}
