package com.vmturbo.group.service;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.DeleteTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc.TopologyDataDefinitionServiceImplBase;
import com.vmturbo.group.topologydatadefinition.TopologyDataDefinitionStore;
import com.vmturbo.identity.exceptions.IdentityStoreException;

/**
 * Implementation of TopologyDataDefinition services.
 */
public class TopologyDataDefinitionRpcService extends TopologyDataDefinitionServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final TopologyDataDefinitionStore topologyDataDefinitionStore;

    /**
     * Create the service.
     *
     * @param topDataDefStore TopologyDataDefinitionStore for persisting topology data definitions.
     */
    public TopologyDataDefinitionRpcService(@Nonnull TopologyDataDefinitionStore topDataDefStore) {
        this.topologyDataDefinitionStore = Objects.requireNonNull(topDataDefStore);
    }

    @Override
    public void createTopologyDataDefinition(
        @Nonnull final CreateTopologyDataDefinitionRequest request,
        @Nonnull final StreamObserver<CreateTopologyDataDefinitionResponse> responseObserver) {
        if (!request.hasTopologyDataDefinition()) {
            final String errMsg =
                "Incoming TopologyDataDefinition create request does not contain a topology data "
                    + "definition.";
            returnErrorResponse(errMsg, Status.INVALID_ARGUMENT, responseObserver, null);
        } else {
            try {
                responseObserver.onNext(CreateTopologyDataDefinitionResponse.newBuilder()
                    .setTopologyDataDefinition(topologyDataDefinitionStore
                        .createTopologyDataDefinition(request.getTopologyDataDefinition()))
                    .build());
                responseObserver.onCompleted();
            } catch (StoreOperationException e) {
                returnErrorResponse(e.getMessage(), e.getStatus(), responseObserver, e);
            } catch (IdentityStoreException e) {
                returnErrorResponse(e.getMessage(), Status.INTERNAL, responseObserver, e);
            }
        }
    }

    @Override
    public void getTopologyDataDefinition(@Nonnull final TopologyDataDefinitionID request,
               @Nonnull final StreamObserver<GetTopologyDataDefinitionResponse> responseObserver) {
        if (!request.hasId()) {
            final String errMsg =
                "Incoming TopologyDataDefinition get request does not contain an ID.";
            returnErrorResponse(errMsg, Status.INVALID_ARGUMENT, responseObserver, null);
        } else {
            Optional<TopologyDataDefinition> optionalTopologyDataDefinition =
                topologyDataDefinitionStore.getTopologyDataDefinition(request.getId());
            if (!optionalTopologyDataDefinition.isPresent()) {
                String msg = "No TopologyDataDefinition exists with ID " + request.getId();
                returnErrorResponse(msg, Status.NOT_FOUND, responseObserver, null);
            } else {
                responseObserver.onNext(GetTopologyDataDefinitionResponse.newBuilder()
                    .setTopologyDataDefinition(TopologyDataDefinitionEntry.newBuilder()
                        .setId(request.getId())
                        .setDefinition(optionalTopologyDataDefinition.get())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void getAllTopologyDataDefinitions(
        @Nonnull final GetTopologyDataDefinitionsRequest request,
        @Nonnull final StreamObserver<GetTopologyDataDefinitionResponse> responseObserver) {
        topologyDataDefinitionStore.getAllTopologyDataDefinitions().forEach(defEntry ->
            responseObserver.onNext(GetTopologyDataDefinitionResponse.newBuilder()
                .setTopologyDataDefinition(defEntry)
                .build()));
        responseObserver.onCompleted();
    }

    @Override
    public void updateTopologyDataDefinition(
        @Nonnull final UpdateTopologyDataDefinitionRequest request,
        @Nonnull final StreamObserver<UpdateTopologyDataDefinitionResponse> responseObserver) {
        if (!request.hasId()) {
            final String errMsg = "Invalid TopologyDataDefinitionID input for topology data "
                + "definition update: No TopologyDataDefinition ID specified";
            returnErrorResponse(errMsg, Status.INVALID_ARGUMENT, responseObserver, null);
        } else if (!request.hasTopologyDataDefinition()) {
            final String errMsg =
                "Invalid new topology data definition for update:"
                    + " No topology data definition was provided";
            returnErrorResponse(errMsg, Status.INVALID_ARGUMENT, responseObserver, null);
        } else {
            try {
                Optional<TopologyDataDefinition> optUpdatedDefinition = topologyDataDefinitionStore
                    .updateTopologyDataDefinition(request.getId(), request.getTopologyDataDefinition());
                if (!optUpdatedDefinition.isPresent()) {
                    final String msg = "No topology data definition found with ID " + request.getId();
                    returnErrorResponse(msg, Status.NOT_FOUND, responseObserver, null);
                } else {
                    responseObserver.onNext(UpdateTopologyDataDefinitionResponse.newBuilder()
                        .setUpdatedTopologyDataDefinition(TopologyDataDefinitionEntry.newBuilder()
                                .setDefinition(optUpdatedDefinition.get())
                                .setId(request.getId()))
                        .build());
                    responseObserver.onCompleted();
                }
            } catch (StoreOperationException e) {
                returnErrorResponse(e.getMessage(), e.getStatus(), responseObserver, e);
            } catch (IdentityStoreException e) {
                returnErrorResponse(e.getMessage(), Status.INVALID_ARGUMENT, responseObserver, e);
            }
        }
    }

    @Override
    public void deleteTopologyDataDefinition(@Nonnull final TopologyDataDefinitionID request,
             @Nonnull final StreamObserver<DeleteTopologyDataDefinitionResponse> responseObserver) {
        if (!request.hasId()) {
            final String errMsg = "Invalid TopologyDataDefinitionID input for topology data "
                + "definition delete: No TopologyDataDefinition ID specified";
            returnErrorResponse(errMsg, Status.INVALID_ARGUMENT, responseObserver, null);
        } else {
            try {
                responseObserver.onNext(DeleteTopologyDataDefinitionResponse.newBuilder()
                        .setDeleted(topologyDataDefinitionStore.deleteTopologyDataDefinition(request.getId()))
                        .build());
                responseObserver.onCompleted();
            } catch (StoreOperationException e) {
                returnErrorResponse(e.getMessage(), e.getStatus(), responseObserver, e);
            }
        }
    }

    private void returnErrorResponse(@Nonnull String errorMessage,
                                     @Nonnull Status status,
                                     @Nonnull StreamObserver<?> responseObserver,
                                     @Nullable Exception exception) {
        if (exception != null) {
            logger.error(Objects.requireNonNull(errorMessage), exception);
        } else {
            logger.error(Objects.requireNonNull(errorMessage));
        }
        responseObserver.onError(status
            .withDescription(errorMessage)
            .asException());
    }
}
