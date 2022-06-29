package com.vmturbo.action.orchestrator.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.atomic.AtomicActionSpecsCache;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec.ActionSpecCase;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.UploadAtomicActionSpecsRequest;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.UploadAtomicActionSpecsRequest.UploadType;
import com.vmturbo.common.protobuf.action.AtomicActionSpecsUploadServiceGrpc.AtomicActionSpecsUploadServiceImplBase;

/**
 * Server (Action Orchestrator) side handler
 * receives the message containing the {@link AtomicActionSpec}
 * AtomicActionSpecsUploadServiceImplBase - The gRPC protobuf compiler plugin generated base class.
 */
public class AtomicActionSpecsRpcService extends AtomicActionSpecsUploadServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    // Cache for saving the atomic action specs by action type
    private final AtomicActionSpecsCache liveAtomicActionSpecsCache;
    private final AtomicActionSpecsCache planAtomicActionSpecsCache;

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
     * @param liveAtomicActionSpecsCache the cache that holds atomic action specs for realtime
     * @param planAtomicActionSpecsCache the cache that holds atomic action specs for plan
     */
    AtomicActionSpecsRpcService(@Nonnull final AtomicActionSpecsCache liveAtomicActionSpecsCache,
                                @Nonnull final AtomicActionSpecsCache planAtomicActionSpecsCache) {
        this.liveAtomicActionSpecsCache = liveAtomicActionSpecsCache;
        this.planAtomicActionSpecsCache = planAtomicActionSpecsCache;
    }

    /**
     * Get the atomic action specs cache by upload type.
     *
     * @param uploadType the upload type
     * @return the atomic action specs cache for the given upload type
     */
    @Nonnull
    private Optional<AtomicActionSpecsCache> getAtomicActionSpecsCache(
            @Nonnull final UploadType uploadType)  {
        switch (uploadType) {
            case REALTIME:
                return Optional.of(liveAtomicActionSpecsCache);
            case PLAN:
                return Optional.of(planAtomicActionSpecsCache);
            default:
                logger.debug("AtomicActionSpecsRpcService : unsupported upload type {}",
                             uploadType);
                return Optional.empty();
        }
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

        private UploadType uploadType;

        /**
         * Constructor of this class.
         *
         * @param responseObserver an empty response observer
         */
        AtomicActionSpecsRequest(StreamObserver<Empty> responseObserver) {
            this.responseObserver = responseObserver;
            this.actionSpecsMap = new HashMap<>();
            this.uploadType = UploadType.REALTIME;
        }

        @Override
        public void onNext(final UploadAtomicActionSpecsRequest request) {
            this.uploadType = request.getUploadType();
            request.getAtomicActionSpecsInfoList().stream()
                    .filter(actionSpec -> actionTypes.containsKey(actionSpec.getActionSpecCase()))
                    .forEach(actionSpec -> {
                        final ActionType actionType = actionTypes.get(actionSpec.getActionSpecCase());
                        // save by action type
                        actionSpecsMap.computeIfAbsent(actionType, v -> new ArrayList<>())
                                .add(actionSpec);
                    });
        }

        @Override
        public void onError(final Throwable throwable) {
            logger.error("Encountered error in uploadAtomicActionSpecs", throwable);
        }

        @Override
        public void onCompleted() {
            logger.debug("AtomicActionSpecsRpcService : updating atomic action specs cache for {}",
                         uploadType);
            //now move to store
            getAtomicActionSpecsCache(uploadType)
                    .ifPresent(cache -> cache.updateAtomicActionSpecsInfo(actionSpecsMap));
            responseObserver.onCompleted();
        }
    }
}
