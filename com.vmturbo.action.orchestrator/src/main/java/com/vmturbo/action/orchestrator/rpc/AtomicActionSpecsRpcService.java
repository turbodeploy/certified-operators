package com.vmturbo.action.orchestrator.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec.ActionSpecCase;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.UploadAtomicActionSpecsRequest;
import com.vmturbo.common.protobuf.action.AtomicActionSpecsUploadServiceGrpc.AtomicActionSpecsUploadServiceImplBase;

/**
 * Server (Action Orchestrator) side handler
 * receives the message containing the {@link AtomicActionSpec}
 * AtomicActionSpecsUploadServiceImplBase - The gRPC protobuf compiler plugin generated base class.
 */
public class AtomicActionSpecsRpcService extends AtomicActionSpecsUploadServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    // Cache for saving the atomic action specs by action type
    private final AtomicActionSpecsCache atomicActionSpecsCache;

    private static final Map<ActionSpecCase, ActionType> actionTypes
                = ImmutableMap.of(
                    ActionSpecCase.MOVESPEC,        ActionType.MOVE,
                    ActionSpecCase.RESIZESPEC,      ActionType.RESIZE,
                    ActionSpecCase.SCALESPEC,       ActionType.SCALE,
                    ActionSpecCase.PROVISIONSPEC,   ActionType.PROVISION
                );

    /**
     * Constructor of the rpc service.
     *
     * @param atomicActionSpecsCache the cache which has access to all atomic action specs
     */
    AtomicActionSpecsRpcService(@Nonnull final AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<UploadAtomicActionSpecsRequest> uploadAtomicActionSpecs(
                    @Nonnull final StreamObserver<Empty> responseObserver) {
        return new AtomicActionSpecsRequest(responseObserver);
    }

    /**
     * A stream observer on which to write chunks of atomic action specs info.
     */
    private class AtomicActionSpecsRequest implements StreamObserver<UploadAtomicActionSpecsRequest> {

        private final StreamObserver<Empty> responseObserver;

        private final Map<ActionType, List<AtomicActionSpec>> actionSpecsMap;

        /**
         * Constructor of this class.
         *
         * @param responseObserver an empty response observer
         */
        AtomicActionSpecsRequest(StreamObserver<Empty> responseObserver) {
            this.responseObserver = responseObserver;
            this.actionSpecsMap = new HashMap<>();
        }

        @Override
        public void onNext(final UploadAtomicActionSpecsRequest request) {
            for (AtomicActionSpec actionSpec : request.getAtomicActionSpecsInfoList()) {

                if (!actionTypes.containsKey(actionSpec.getActionSpecCase())) {
                    logger.warn("Received action spec for unsupported action type {}",
                                                actionSpec.getActionSpecCase());
                    continue;
                }

                ActionType actionType = actionTypes.get(actionSpec.getActionSpecCase());

               // save by action type
               actionSpecsMap.computeIfAbsent(actionType, v -> new ArrayList<>())
                                                    .add(actionSpec);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            logger.error("Encountered error in uploadAtomicActionSpecs", throwable);
        }

        @Override
        public void onCompleted() {
            logger.debug("AtomicActionSpecsRpcService : updating atomic action specs cache");
            //now move to store
            atomicActionSpecsCache.updateAtomicActionSpecsInfo(actionSpecsMap);
            responseObserver.onCompleted();
        }
    }
}
