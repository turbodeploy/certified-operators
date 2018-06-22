package com.vmturbo.action.orchestrator.rpc;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCount;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.SeverityCountsResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceImplBase;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;

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
                                    StreamObserver<EntitySeveritiesResponse> responseObserver) {
        final Optional<EntitySeverityCache> optionalCache =
            actionStorehouse.getSeverityCache(request.getTopologyContextId());
        if (!request.hasPaginationParams()) {
            EntitySeveritiesResponse.Builder responseBuilder = EntitySeveritiesResponse.newBuilder();
            request.getEntityIdsList().stream()
                    .map(oid -> generateEntitySeverity(oid, optionalCache))
                    .forEach(responseBuilder::addEntitySeverity);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }
        final PaginationParameters paginationParameters = request.getPaginationParams();
        if (paginationParameters.getLimit() <= 0) {
            throw new IllegalArgumentException("Illegal pagination limit: " +
                    paginationParameters.getLimit() + " must be a positive integer");
        }
        final long skipCount = paginationParameters.hasCursor() ? Long.valueOf(paginationParameters.getCursor()) : 0;
        final Comparator<Long> comparator = getComparator(optionalCache);
        final List<EntitySeverity> results = request.getEntityIdsList().stream()
                .sorted(paginationParameters.getAscending() ? comparator : comparator.reversed())
                .skip(skipCount)
                .limit(paginationParameters.getLimit())
                .map(oid -> generateEntitySeverity(oid, optionalCache))
                .collect(Collectors.toList());
        EntitySeveritiesResponse.Builder responseBuilder =
                EntitySeveritiesResponse.newBuilder()
                        .addAllEntitySeverity(results)
                        .setPaginationResponse(PaginationResponse.newBuilder());
        if ((skipCount + results.size()) < request.getEntityIdsCount()) {
            // if there are more results left.
            responseBuilder.getPaginationResponseBuilder()
                    .setNextCursor(String.valueOf(skipCount + paginationParameters.getLimit()));
        }
        responseObserver.onNext(responseBuilder.build());
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

    /**
     * Create a comparator to sort entity oid, if there is related entity severity cached, use its
     * entity severity as comparator, otherwise, use the entity oid as the default comparator.
     *
     * @param optionalCache {@link EntitySeverityCache}
     * @return a comparator to sort entity oids.
     */
    private Comparator<Long> getComparator(@Nonnull final Optional<EntitySeverityCache> optionalCache) {
        if (!optionalCache.isPresent()) {
            // if there is no severity cache, use the oid order as default comparator.
            return Comparator.comparingLong(Long::valueOf);
        }
        final EntitySeverityCache severityCache = optionalCache.get();
        return Comparator.comparing(entity ->
                severityCache.getSeverity(entity).orElse(Severity.NORMAL));
    }

    /**
     * Generate a {@link EntitySeverity} based on entity oid and related entity severity information.
     *
     * @param oid oid of entity.
     * @param optionalCache {@link EntitySeverityCache}.
     * @return a {@link EntitySeverity}.
     */
    private EntitySeverity generateEntitySeverity(
            @Nonnull final long oid,
            @Nonnull final Optional<EntitySeverityCache> optionalCache) {
        final EntitySeverity.Builder entitySeverityBuilder = EntitySeverity.newBuilder()
                .setEntityId(oid);
        final Optional<Severity> severityOptional =
                optionalCache.isPresent() && optionalCache.get().getSeverity(oid).isPresent()
                ? Optional.of(optionalCache.get().getSeverity(oid).get())
                : Optional.empty();
        severityOptional.ifPresent(entitySeverityBuilder::setSeverity);
        return entitySeverityBuilder.build();
    }
}
