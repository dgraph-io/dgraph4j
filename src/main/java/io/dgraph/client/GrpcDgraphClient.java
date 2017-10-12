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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dgraph.entity.CommonConstants;
import io.dgraph.entity.DgraphRequest;
import io.dgraph.entity.Node;
import io.dgraph.exception.DGraphException;
import io.dgraph.proto.AssignedIds;
import io.dgraph.proto.DgraphGrpc;
import io.dgraph.proto.Num;
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

    private Allocator allocator;

    private GrpcDgraphClient(final String theHostname, final int thePort) {
        allocator = new Allocator();
        channel = ManagedChannelBuilder.forAddress(theHostname, thePort).usePlaintext(true).build();
        blockingStub = DgraphGrpc.newBlockingStub(channel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GrpcDgraphResult query(final String theQuery) {
        try {
            logger.debug("Starting query...");
            final Request aRequest = Request.newBuilder().setQuery(theQuery).build();

            logger.debug("Sending request to Dgraph...");
            final Response aResponse = blockingStub.run(aRequest);
            logger.debug("Recevied response from Dgraph!!");

            logger.debug("Latency: " + aResponse.getL().toString());
            return GrpcDgraphResult.newInstance(aResponse);
        } catch (RuntimeException re) {
            re.printStackTrace();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AssignedIds assignUid(final long count) {
        try {
            logger.debug("Starting query...");

            logger.debug("Sending request to Dgraph...");
            Num request = Num.newBuilder().setVal(count).build();
            final AssignedIds aResponse = blockingStub.assignUids(request);
            logger.debug("Recevied response from Dgraph!!");

            return aResponse;
        } catch (RuntimeException re) {
            re.printStackTrace();
        }
        return null;
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

    @Override
    public DgraphResult query(DgraphRequest request) {

        logger.debug("Sending request to Dgraph...");
        final Response aResponse = blockingStub.run(request.getGr());
        logger.debug("Recevied response from Dgraph!!");

        logger.debug("Latency: " + aResponse.getL().toString());
        return GrpcDgraphResult.newInstance(aResponse);
    }

    @Override
    public Node NodeBlank(String varName) {
        Node node = null;
        if (StringUtils.isBlank(varName)) {
            try {
                allocator.lock();
                long uid = fetchOne();
                node = new Node(uid, null);

            } finally {
                allocator.unlock();

            }
        }

        long uid = assignOrGet(CommonConstants.VAR_PREFIX + varName);
        node = new Node(uid, null);
        return node;

    }

    @Override
    public Node NodeUidVar(String name) {
        if (StringUtils.isBlank(name)) {
            throw new DGraphException("Name is null");
        }

        return new Node(name);
    }

    private long fetchOne() {

        if (!allocator.isHeldByCurrentThread()) {
            throw new DGraphException("Lock not acquired");
        }

        if (allocator.getStartId() == 0 || allocator.getStartId() > allocator.getEndId()) {
            while (true) {
                try {
                    AssignedIds assingedIds = assignUid(1000);
                    allocator.setStartId(assingedIds.getStartId());
                    allocator.setEndId(assingedIds.getEndId());
                    break;

                } catch (Exception e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {

                    }
                }
            }
        }

        long resp = allocator.incrStartId();
        return resp;

    }

    private long assignOrGet(String id) {
        allocator.lock();
        long uid = allocator.cacheGet(id);
        if (uid > 0) {
            // return uid;
        } else {
            uid = fetchOne();
        }

        allocator.unlock();
        return uid;
    }

}
