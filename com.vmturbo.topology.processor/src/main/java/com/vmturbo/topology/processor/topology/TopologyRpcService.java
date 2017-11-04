package com.vmturbo.topology.processor.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastResponse;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceImplBase;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Implementation of the TopologyService defined in topology/TopologyDTO.proto.
 */
public class TopologyRpcService extends TopologyServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;
    private final TargetStore targetStore;

    public TopologyRpcService(@Nonnull final TopologyHandler topologyHandler,
                              @Nonnull final TargetStore targetStore) {
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    @Override
    public void requestTopologyBroadcast(TopologyBroadcastRequest request,
                                         StreamObserver<TopologyBroadcastResponse> responseObserver) {
        try {
            topologyHandler.broadcastLatestTopology(targetStore);
            responseObserver.onNext(TopologyBroadcastResponse.newBuilder()
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Unable to broadcast topology due to error: ", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getMessage())
                .withCause(e)
                .asException()
            );
        }
    }
}
