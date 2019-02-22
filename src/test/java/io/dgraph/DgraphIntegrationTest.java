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

import static org.testng.Assert.fail;

import io.dgraph.DgraphProto.Operation;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class DgraphIntegrationTest {
  protected static final Logger logger = LoggerFactory.getLogger(DgraphIntegrationTest.class);
  private static ManagedChannel channel;
  protected static DgraphClient dgraphClient;

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
    dgraphClient = new DgraphClient(stub);
    boolean succeed = false;
    boolean retriable;
    do {
      retriable = false;

      try {
        // since the cluster is run with ACL turned on by default,
        // we need to login as groot to perform arbitrary operations
        dgraphClient.login("groot", "password");
        succeed = true;
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

    if (!succeed) {
      fail("Unable to perform the DropAll operation");
    }
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @AfterClass
  public static void afterClass() throws InterruptedException, IOException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
