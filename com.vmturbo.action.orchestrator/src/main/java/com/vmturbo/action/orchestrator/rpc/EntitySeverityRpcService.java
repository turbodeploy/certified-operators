package com.vmturbo.action.orchestrator.rpc;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class EntitySeverityRpcService extends EntitySeverityServiceImplBase {

    private final ActionStorehouse actionStorehouse;

    /**
     * Create a new ActionsRpcService.
     *  @param actionStorehouse The storehouse containing action stores.
     */
    public EntitySeverityRpcService(@Nonnull final ActionStorehouse actionStorehouse) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntitySeverities(MultiEntityRequest request,
                                    StreamObserver<EntitySeverity> responseObserver) {
        Optional<EntitySeverityCache> cache = actionStorehouse.getSeverityCache(request.getTopologyContextId());
        if (!cache.isPresent()) {
            // Nothing to return
            responseObserver.onCompleted();
            return;
        }

        request.getEntityIdsList().stream()
            .map(oid -> {
                EntitySeverity.Builder builder = EntitySeverity.newBuilder()
                    .setEntityId(oid);

                cache.get()
                    .getSeverity(oid)
                    .ifPresent(builder::setSeverity);

                return builder.build();
            }).forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }
}
