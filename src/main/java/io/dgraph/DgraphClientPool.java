/*
 * Copyright 2016-18 DGraph Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dgraph;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javafx.util.Pair;

/**
 * Manage a pool of Dgraph clients.
 *
 * <p>Clients must belong to the same Dgraph cluster
 *
 * @author Deepak Jois
 */
public class DgraphClientPool {
  List<Pair<ManagedChannel, DgraphGrpc.DgraphBlockingStub>> clients;

  private int deadlineSecs;

  private DgraphClientPool() {
    this.clients = new ArrayList<>();
  }

  /**
   * Create a pool of clients from a list of {@link ManagedChannel} objects.
   *
   * @param channels List of {@link ManagedChannel} objects to use when creating GRPC clients
   */
  public DgraphClientPool(List<ManagedChannel> channels) {
    this();
    add(channels);
  }

  /**
   * Create a pool of clients from a list of {@link ManagedChannel} objects, and specify a deadline
   * (in seconds) for the requests to execute.
   *
   * @param channels List of {@link ManagedChannel} objects to use when creating GRPC clients
   * @param deadlineSecs deadline in seconds for requests made to the clients.
   */
  public DgraphClientPool(List<ManagedChannel> channels, int deadlineSecs) {
    this(channels);
    this.deadlineSecs = deadlineSecs;
  }

  /**
   * Add a client that connects to a Dgraph server located at specified host and port.
   *
   * @param host
   * @param port
   */
  public void add(String host, int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
    DgraphGrpc.DgraphBlockingStub stub = DgraphGrpc.newBlockingStub(channel);
    clients.add(new Pair<>(channel, stub));
  }

  /**
   * Add a client that connects to a Dgraph server, using the specified {@link ManagedChannel}.
   *
   * @param channel
   */
  public void add(ManagedChannel channel) {
    DgraphGrpc.DgraphBlockingStub stub = DgraphGrpc.newBlockingStub(channel);
    clients.add(new Pair<>(channel, stub));
  }

  /**
   * Add clients that connect to different nodes in a Dgraph cluster, using the specified {@link
   * ManagedChannel} objects.
   *
   * @param channels
   */
  public void add(List<ManagedChannel> channels) {
    for (ManagedChannel channel : channels) {
      DgraphGrpc.DgraphBlockingStub stub = DgraphGrpc.newBlockingStub(channel);
      clients.add(new Pair<>(channel, stub));
    }
  }

  /**
   * Returns a randomly chosen client from the pool.
   *
   * @return A {@link io.dgraph.DgraphGrpc.DgraphBlockingStub} object.
   */
  public DgraphGrpc.DgraphBlockingStub anyClient() {
    Random rand = new Random();

    DgraphGrpc.DgraphBlockingStub client = clients.get(rand.nextInt(clients.size())).getValue();

    if (deadlineSecs > 0) {
      return client.withDeadlineAfter(deadlineSecs, TimeUnit.SECONDS);
    }

    return client;
  }

  /**
   * Return the deadline(in secs) for the clients in the pool.
   *
   * @return
   */
  public int getDeadline() {
    return deadlineSecs;
  }

  /**
   * Set the deadline for the clients in the pool in secs.
   *
   * @param deadlineSecs
   */
  public void setDeadline(int deadlineSecs) {
    this.deadlineSecs = deadlineSecs;
  }

  /**
   * Closes all the clients in the pool with the specified timeout.
   *
   * @param timeoutSecs timeout value in seconds
   * @throws InterruptedException
   */
  public void close(long timeoutSecs) throws InterruptedException {
    for (Pair<ManagedChannel, DgraphGrpc.DgraphBlockingStub> client : clients) {
      client.getKey().awaitTermination(timeoutSecs, TimeUnit.SECONDS);
    }
  }

  /**
   * Closes all the clients in the pool with a default timeout of 5 seconds.
   *
   * @throws InterruptedException
   */
  public void close() throws InterruptedException {
    close(5);
  }
}
