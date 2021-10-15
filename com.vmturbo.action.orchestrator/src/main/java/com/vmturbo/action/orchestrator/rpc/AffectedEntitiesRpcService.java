package com.vmturbo.action.orchestrator.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.execution.affected.entities.AffectedEntitiesManager;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.ActionEffectType;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.AffectedEntitiesTimeoutConfig;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.GetAffectedEntitiesRequest;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.GetAffectedEntitiesResponse;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.GetAffectedEntitiesResponse.AffectedEntities;
import com.vmturbo.common.protobuf.action.AffectedEntitiesServiceGrpc.AffectedEntitiesServiceImplBase;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving the affected entities.
 */
public class AffectedEntitiesRpcService extends AffectedEntitiesServiceImplBase {

    private static final Logger logger = LogManager.getLogger();
    private AffectedEntitiesManager affectedEntitiesManager;

    /**
     * Creates an instance of the service with the provided dependencies.
     *
     * @param affectedEntitiesManager the object that manages affected actions.
     */
    public AffectedEntitiesRpcService(
            @Nonnull final AffectedEntitiesManager affectedEntitiesManager) {
        this.affectedEntitiesManager = Objects.requireNonNull(affectedEntitiesManager);
    }

    /**
     * Translates the request and sends it to the manager. Then translates the response from
     * the manager.
     *
     * @param request request translate and send to the manager.
     * @param responseObserver the response converted from the manager.
     */
    @Override
    public void getAffectedEntities(final GetAffectedEntitiesRequest request,
            final StreamObserver<GetAffectedEntitiesResponse> responseObserver) {
        try {
            Map<Integer, AffectedEntitiesTimeoutConfig> rawInput = request.getRequestedAffectionsMap();
            Map<ActionEffectType, AffectedEntitiesTimeoutConfig> translatedInput = new HashMap<>();
            for (Map.Entry<Integer, AffectedEntitiesTimeoutConfig> entry : rawInput.entrySet()) {
                translatedInput.put(
                        ActionEffectType.forNumber(entry.getKey()),
                        entry.getValue());
            }
            Map<ActionEffectType, Set<Long>> rawResult = affectedEntitiesManager.getAllAffectedEntities(translatedInput);
            GetAffectedEntitiesResponse.Builder translatedResult = GetAffectedEntitiesResponse.newBuilder();
            for (Map.Entry<ActionEffectType, Set<Long>> entry : rawResult.entrySet()) {
                translatedResult.putAffectedEntities(
                        entry.getKey().getNumber(),
                        AffectedEntities.newBuilder().addAllEntityId(entry.getValue()).build());
            }
            responseObserver.onNext(translatedResult.build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            logger.error("Failed to get affected entities. " + request.toString(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(
                    "Failed to get affected entities." + e.getMessage()).asException());
        }
    }
}
