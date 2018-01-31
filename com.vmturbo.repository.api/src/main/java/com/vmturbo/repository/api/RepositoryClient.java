package com.vmturbo.repository.api;

import java.util.Iterator;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;

/**
 * Sends commands to the repository using gRPC.
 */
public class RepositoryClient {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryClient.class);

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

    @Nonnull
    public Iterator<PlanEntityStats> getPlanStats(@Nonnull PlanTopologyStatsRequest request) {
        return repositoryService.getPlanTopologyStats(request);
    }
}
