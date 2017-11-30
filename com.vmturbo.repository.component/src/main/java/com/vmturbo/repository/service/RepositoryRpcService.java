package com.vmturbo.repository.service;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDBException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufHandler;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Server side implementation of the repository gRPC calls.
 */
public class RepositoryRpcService extends RepositoryServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyProtobufsManager topologyProtobufsManager;

    public RepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                @Nonnull final TopologyProtobufsManager topologyProtobufsManager) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.topologyProtobufsManager = Objects.requireNonNull(topologyProtobufsManager);
    }

    private boolean validateDeleteTopologyRequest(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {

        if (!request.hasTopologyId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Context ID missing")
                .asException());
            return false;
        }

        return true;
    }

    @Override
    public void deleteTopology(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {
        if (!validateDeleteTopologyRequest(request, responseObserver)) {
            return;
        }

        logger.info("Deleting topology with id:{} and contextId:{} ",
                request.getTopologyId(), request.getTopologyContextId());
        try {
            topologyLifecycleManager.deleteTopology(
                    new TopologyID(request.getTopologyContextId(),
                            request.getTopologyId(),
                            TopologyType.PROJECTED));
            final RepositoryOperationResponse responseBuilder =
                    RepositoryOperationResponse.newBuilder()
                        .setResponseCode(RepositoryOperationResponseCode.OK)
                        .build();
            responseObserver.onNext(responseBuilder);
            responseObserver.onCompleted();

        } catch (TopologyDeletionException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        final long topologyID = topologyRequest.getTopologyId();
        try {
            logger.info("Retrieving topology for {} with filtere {}", topologyID,
                    topologyRequest.getEntityFilter());
            final TopologyProtobufReader reader =
                    topologyProtobufsManager.createTopologyProtobufReader(
                        topologyRequest.getTopologyId(),
                        topologyRequest.hasEntityFilter() ?
                                Optional.of(topologyRequest.getEntityFilter()) : Optional.empty());
            while (reader.hasNext()) {
                List<TopologyEntityDTO> chunk = reader.nextChunk();
                final RepositoryDTO.RetrieveTopologyResponse responseChunk =
                                RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                                        .addAllEntities(chunk)
                                        .build();
                responseObserver.onNext(responseChunk);
            }
            responseObserver.onCompleted();
        } catch (NoSuchElementException nse) {
            responseObserver.onError(Status
                .NOT_FOUND
                .withDescription(String.format("Cannot find topology with ID %s", topologyID))
                .asException());
        } catch (RuntimeException e) {
            responseObserver.onError(Status.UNKNOWN.withCause(e)
                                                   .withDescription(e.getMessage())
                                                   .asException());
        }
    }

}
