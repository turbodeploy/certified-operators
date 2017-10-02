package com.vmturbo.repository.service;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDBException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Server side implementation of the repository gRPC calls.
 */
public class RepositoryRpcService extends RepositoryServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private final TopologyProtobufsManager topoManager;
    private final TopologyEventHandler topologyEventHandler;
    // This is ugly. Arango doesn't have an api for getting the
    // error code. so have to hardcode it here.
    // There is ErroNums class in 3.x but not in 4.x
    // http://arangodb.github.io/arangodb-java-driver/javadoc-3_0/com/arangodb/ErrorNums.html
    // http://arangodb.github.io/arangodb-java-driver/javadoc-4_0/index.html?overview-summary.html
    public static final int ERROR_ARANGO_DATABASE_NOT_FOUND = 1228;

    public RepositoryRpcService(TopologyProtobufsManager tpm, TopologyEventHandler topologyEventHandler) {
        this.topoManager = Objects.requireNonNull(tpm);
        this.topologyEventHandler = Objects.requireNonNull(topologyEventHandler);
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

        try {
            if (!validateDeleteTopologyRequest(request, responseObserver)) {
                return;
            }

            // Delete the raw topology protobuf from the collection. The raw
            // protobuf is stored for plan-over-plan use case (otherwise the
            // graph representation has to be again converted back to the
            // Topology protobuf format)
            logger.info("Deleting topology with id:{} and contextId:{} ",
                    request.getTopologyId(), request.getTopologyContextId());
            topoManager.createTopologyProtobufReader(request.getTopologyId()).delete();
            // Delete the associated graph representation of the topology
            boolean wasDropped = topologyEventHandler.dropDatabase(
                    new TopologyID(request.getTopologyContextId(),
                        request.getTopologyId(),
                        TopologyType.PROJECTED));

            RepositoryOperationResponse.Builder responseBuilder =
                RepositoryOperationResponse.newBuilder();

            if (wasDropped) {
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.OK);
            } else {
                responseBuilder.setResponseCode(RepositoryOperationResponseCode.FAILED);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            String errMsg = "Cannot find rawTopology with topologyId: " +
                request.getTopologyId() + " and topologyContextId: " +
                request.getTopologyContextId();
            logger.info(errMsg);
            responseObserver.onError(Status
                .NOT_FOUND
                .withDescription(errMsg)
                .asException());
        } catch (ArangoDBException arge) {
            if (arge.getErrorNum() == ERROR_ARANGO_DATABASE_NOT_FOUND) {
                String errMsg = "Cannot find topologyGraph with topologyId: " +
                    request.getTopologyId() + " and topologyContextId: " +
                    request.getTopologyContextId();
                logger.info(errMsg);
                responseObserver.onError(Status
                    .NOT_FOUND
                    .withDescription(errMsg)
                    .asException());
            } else {
                throw arge;
            }
        }
    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        final long topologyID = topologyRequest.getTopologyId();
        try {
            logger.info("Retrieving topology for {}", topologyID);
            TopologyProtobufReader reader = topoManager.createTopologyProtobufReader(topologyRequest.getTopologyId());
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
