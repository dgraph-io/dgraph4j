/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import io.dgraph.DgraphProto.Operation;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

public abstract class DgraphIntegrationTest {
  protected static final Logger logger = LoggerFactory.getLogger(DgraphIntegrationTest.class);
  private static ManagedChannel channel;
  protected static DgraphClient dgraphClient;

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;

  @BeforeSuite
  public static void setupCluster() throws IOException, InterruptedException {
    System.out.println("Starting the cluster");
    TestUtils.checkCmd(
        "unable to start the cluster",
        "docker-compose",
        "-f",
        System.getenv("GOPATH") + "/src/github.com/dgraph-io/dgraph/dgraph/docker-compose.yml",
        "up",
        "--force-recreate",
        "--remove-orphans",
        "--detach");
    System.out.println("Cluster setup complete. Sleeping for 10s for cluster to stabilize");
    // sleep for 10 seconds for the cluster to stablize
    Thread.sleep(10 * 1000);
  }

  @AfterSuite
  public static void tearDownCluster() throws IOException, InterruptedException {
    System.out.println("Tearing down the cluser");
    TestUtils.checkCmd(
        "unable to start the cluster",
        "docker-compose",
        "-f",
        System.getenv("GOPATH") + "/src/github.com/dgraph-io/dgraph/dgraph/docker-compose.yml",
        "down");
    System.out.println("Cluster tear-down complete.");
  }

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
    dgraphClient = new DgraphClient(stub);

    boolean retriable;
    do {
      retriable = false;

      try {
        dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
      } catch (RuntimeException e) {
        // check if the error is retriable
        Throwable t = e;
        while (t != null) {
          if (t.getMessage().contains("Please retry")) {
            retriable = true;
            break;
          }
          t = t.getCause();
        }
      }

      if (retriable) {
        System.out.println("Receiveb retriable error, will retry after 1s");
        Thread.sleep(1000);
      }
    } while (retriable);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, IOException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
