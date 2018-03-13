package com.vmturbo.action.orchestrator.rpc;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class EntitySeverityRpcService extends EntitySeverityServiceImplBase {

    private final ActionStorehouse actionStorehouse;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new ActionsRpcService.
     *
     * @param actionStorehouse The storehouse containing action stores.
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
        final Optional<EntitySeverityCache> optionalCache =
            actionStorehouse.getSeverityCache(request.getTopologyContextId());
        if (!optionalCache.isPresent()) {
            // The contract is that every requested entity must be included in the response.
            request.getEntityIdsList().stream()
                .map(oid -> EntitySeverity.newBuilder()
                    .setEntityId(oid)
                    .build())
                .forEach(responseObserver::onNext);


            responseObserver.onCompleted();
            return;
        } else {
            final EntitySeverityCache cache = optionalCache.get();

            request.getEntityIdsList().stream()
                .map(oid -> {
                    EntitySeverity.Builder builder = EntitySeverity.newBuilder()
                        .setEntityId(oid);

                    cache.getSeverity(oid)
                        .ifPresent(builder::setSeverity);

                    return builder.build();
                }).forEach(responseObserver::onNext);
        }

        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSeverityCounts(MultiEntityRequest request,
                                  StreamObserver<SeverityCountsResponse> responseObserver) {
        final Optional<EntitySeverityCache> optionalCache =
            actionStorehouse.getSeverityCache(request.getTopologyContextId());
        if (!optionalCache.isPresent()) {
            // Missing entities are regarded as having an unknown status.
            responseObserver.onNext(
                SeverityCountsResponse.newBuilder()
                    .setUnknownEntityCount(request.getEntityIdsCount())
                    .build());
        } else {
            final Map<Optional<Severity>, Long> severities = optionalCache.get().getSeverityCounts(
                request.getEntityIdsList());
            final SeverityCountsResponse.Builder builder = SeverityCountsResponse.newBuilder();
            severities.forEach((severity, count) -> {
                    if (severity.isPresent()) {
                        builder.addCounts(SeverityCount.newBuilder()
                            .setEntityCount(count)
                            .setSeverity(severity.get()));
                    } else {
                        builder.setUnknownEntityCount(count);
                    }
                });
            responseObserver.onNext(builder.build());
        }

        responseObserver.onCompleted();
    }
}
