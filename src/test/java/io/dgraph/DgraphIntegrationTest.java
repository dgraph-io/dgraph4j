package io.dgraph;

import io.dgraph.DgraphGrpc.DgraphBlockingStub;
import io.dgraph.DgraphProto.Operation;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DgraphIntegrationTest {
  protected static final Logger logger = LoggerFactory.getLogger(DgraphIntegrationTest.class);
  private static ManagedChannel channel;
  protected static DgraphClient dgraphClient;

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9080;

  @BeforeClass
  public static void beforeClass() {

    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
    dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));

    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
