package com.vmturbo.action.orchestrator.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.UploadActionConstraintInfoRequest;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc.ActionConstraintsServiceImplBase;

/**
 * This service provides RPCs for CRUD-type operations related to action constraints.
 */
public class ActionConstraintsRpcService extends ActionConstraintsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final ActionConstraintStoreFactory actionConstraintStoreFactory;

    /**
     * This is the constructor of this class.
     *
     * @param actionConstraintStoreFactory the factory which has access to all action constraint stores
     */
    ActionConstraintsRpcService(@Nonnull final ActionConstraintStoreFactory actionConstraintStoreFactory) {
        this.actionConstraintStoreFactory = actionConstraintStoreFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<UploadActionConstraintInfoRequest> uploadActionConstraintInfo(
            @Nonnull final StreamObserver<Empty> responseObserver) {
        return new ActionConstraintInfoRequest(responseObserver);
    }

    /**
     * A stream observer on which to write chunks of action constraint info.
     */
    private class ActionConstraintInfoRequest implements StreamObserver<UploadActionConstraintInfoRequest> {

        private final StreamObserver<Empty> responseObserver;

        // All action constraint info, across all chunks.
        // This temporary variable is needed because later we'll clear the actionConstraintStore
        // and insert this new action constraint info into it.
        private final Map<ActionConstraintType, ActionConstraintInfo.Builder> actionConstraintInfoMap;

        /**
         * The constructor of this class.
         *
         * @param responseObserver an empty response observer
         */
        ActionConstraintInfoRequest(StreamObserver<Empty> responseObserver) {
            this.responseObserver = responseObserver;
            actionConstraintInfoMap = new HashMap<>();
        }

        @Override
        public void onNext(final UploadActionConstraintInfoRequest request) {
            for (ActionConstraintInfo constraintInfo : request.getActionConstraintInfoList()) {
                actionConstraintInfoMap.computeIfAbsent(constraintInfo.getActionConstraintType(),
                    key -> ActionConstraintInfo.newBuilder()).mergeFrom(constraintInfo);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            logger.error("Encountered error in uploadActionConstraintInfo", throwable);
        }

        @Override
        public void onCompleted() {
            responseObserver.onCompleted();
            actionConstraintStoreFactory.updateActionConstraintInfo(
                actionConstraintInfoMap.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build())));
        }
    }
}
