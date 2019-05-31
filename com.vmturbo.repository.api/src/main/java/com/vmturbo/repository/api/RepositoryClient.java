package com.vmturbo.repository.api;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Sends commands to the repository using gRPC.
 */
public class RepositoryClient {

    private static final Logger logger = LogManager.getLogger(RepositoryClient.class);

    private final RepositoryServiceBlockingStub repositoryService;

    public RepositoryClient(@Nonnull Channel repositoryChannel) {
        repositoryService = RepositoryServiceGrpc.newBlockingStub(Objects.requireNonNull(repositoryChannel));
    }

    public Iterator<RetrieveTopologyResponse> retrieveTopology(long topologyId) {
        RetrieveTopologyRequest request = RetrieveTopologyRequest.newBuilder()
                .setTopologyId(topologyId)
                .build();
        return repositoryService.retrieveTopology(request);
    }

    /**
     * Retrieve real time topology entities with provided OIDs.
     *
     * @param oids OIDs to retrieve topology entities
     * @param realtimeContextId real time context id
     * @return a stream of {@link TopologyEntityDTO} objects
     */
    public Stream<TopologyEntityDTO> retrieveTopologyEntities(@Nonnull final List<Long> oids,
                                                              final long realtimeContextId) {
        // If we ever need to retrieve ALL topology entities, create a new call with no oids input.
        // It's too error-prone to pass in an empty list to indicate "get all" - we end up
        // getting the entire topology in places we don't need it.
        if (oids.isEmpty()) {
            return Stream.empty();
        }
        RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityOids(oids)
                .setTopologyContextId(realtimeContextId)
                .setTopologyType(TopologyType.SOURCE)
                .build();

        Iterator<EntityBatch> batchIterator = repositoryService.retrieveTopologyEntities(request);
        return RepositoryDTOUtil.topologyEntityStream(batchIterator);
    }

    public RepositoryOperationResponse deleteTopology(long topologyId,
            long topologyContextId) {

        DeleteTopologyRequest request = DeleteTopologyRequest.newBuilder()
                                            .setTopologyId(topologyId)
                                            .setTopologyContextId(topologyContextId)
                                            .build();
        try {
            return repositoryService.deleteTopology(request);
        } catch (StatusRuntimeException sre) {

            RepositoryOperationResponse.Builder responseBuilder =
                RepositoryOperationResponse.newBuilder();

            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // If topology doesn't exist, return success
                logger.info("Topology with Id:{} and contextId:{} not found",
                    topologyId, topologyContextId);
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.OK);
            } else {
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.FAILED)
                    .setError(sre.toString());
            }

            return responseBuilder.build();
        }
    }
}
