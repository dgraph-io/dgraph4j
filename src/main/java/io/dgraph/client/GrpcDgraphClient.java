/*
 * Copyright 2016 DGraph Labs, Inc.
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

package io.dgraph.client;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dgraph.proto.DgraphGrpc;
import io.dgraph.proto.Request;
import io.dgraph.proto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Implementation of a DgraphClient using Grpc.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public class GrpcDgraphClient implements DgraphClient {

    private static final Logger logger = LoggerFactory.getLogger(GrpcDgraphClient.class);

    private final ManagedChannel channel;
    private final DgraphGrpc.DgraphBlockingStub blockingStub;
    public static final int MAX_MESSAGE_SIZE = 32 * 1024 * 1024;

    private GrpcDgraphClient(final String theHostname, final int thePort) {
        channel = ManagedChannelBuilder.forAddress(theHostname, thePort).usePlaintext(true).build();
        blockingStub = DgraphGrpc.newBlockingStub(channel).
                withMaxInboundMessageSize(MAX_MESSAGE_SIZE).
                withMaxOutboundMessageSize(MAX_MESSAGE_SIZE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GrpcDgraphResult query(final String theQuery) {
        int cnt = 0;
        while (true) {
            try {
                logger.debug("Starting query...");
                final Request aRequest = Request.newBuilder().setQuery(theQuery).build();
                logger.debug("Sending request to Dgraph...");
                final Response aResponse = blockingStub.run(aRequest);
                logger.debug("Recevied response from Dgraph!!");

                logger.debug("Latency: " + aResponse.getL().toString());
                return GrpcDgraphResult.newInstance(aResponse);
            } catch (RuntimeException re) {
                logger.warn("Request error - retrying: " + re.getMessage());
                sleep();
                ++cnt;
                if (cnt > 300) {
                    break;
                }
            }
        }
        return null;
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public static DgraphClient newInstance(final String theHostname, final int thePort) {
        return new GrpcDgraphClient(theHostname, thePort);
    }
}
