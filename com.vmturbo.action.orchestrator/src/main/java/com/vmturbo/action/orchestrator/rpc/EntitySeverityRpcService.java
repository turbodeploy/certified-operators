package com.vmturbo.action.orchestrator.rpc;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesChunk;
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

    private final int entitySeverityDefaultPaginationLimit;

    private final int entitySeverityDefaultPaginationMax;

    private final int maxAmountOfEntitiesPerGrpcMessage;

    /**
     * Create a new ActionsRpcService.
     *
     * @param actionStorehouse The storehouse containing action stores.
     * @param entitySeverityDefaultPaginationLimit The pagination limit for entity severities.
     * @param defaultPaginationMax The default pagination limit for a response.
     * @param maxAmountOfEntitiesPerGrpcMessage The maximum amount of entities we can put in a
     *                                          single rpc message.
     */
    public EntitySeverityRpcService(@Nonnull final ActionStorehouse actionStorehouse,
                                    final int entitySeverityDefaultPaginationLimit,
                                    final int defaultPaginationMax,
                                    final int maxAmountOfEntitiesPerGrpcMessage) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.entitySeverityDefaultPaginationLimit = entitySeverityDefaultPaginationLimit;
        this.entitySeverityDefaultPaginationMax = defaultPaginationMax;
        this.maxAmountOfEntitiesPerGrpcMessage = maxAmountOfEntitiesPerGrpcMessage;
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
            Stream<EntitySeverity> severityList = request.getEntityIdsList().stream()
                .map(oid -> generateEntitySeverity(oid, optionalCache));
            // send the results in batches, if needed
            Iterators.partition(severityList.iterator(), maxAmountOfEntitiesPerGrpcMessage)
                .forEachRemaining(chunk -> {
                    EntitySeveritiesResponse.Builder responseBuilder = EntitySeveritiesResponse.newBuilder();
                    responseBuilder.setEntitySeverity(EntitySeveritiesChunk.newBuilder().addAllEntitySeverity(chunk).build());
                    responseObserver.onNext(responseBuilder.build());
                });
            responseObserver.onCompleted();
            return;
        }

        if (request.getPaginationParams().hasLimit() &&
            request.getPaginationParams().getLimit() <= 0) {
            throw new IllegalArgumentException("Illegal pagination limit: " +
                request.getPaginationParams().getLimit() + " must be be a positive integer");
        }

        final PaginationParameters paginationParameters = resetPaginationWithDefaultLimit(request);
        final long skipCount = paginationParameters.hasCursor() ? Long.valueOf(paginationParameters.getCursor()) : 0;
        final List<Long> sortedEntityOids;
        if (optionalCache.isPresent()) {
            // Compare entity severity if entity severity cached is present.
            sortedEntityOids = optionalCache.get().sortEntityOids(
                request.getEntityIdsList(), paginationParameters.getAscending());
        } else {
            // Compare entity oids directly.
            final Comparator<Long> comparator = Long::compare;
            sortedEntityOids = request.getEntityIdsList().stream()
                .sorted(paginationParameters.getAscending() ?
                    comparator : comparator.reversed())
                .collect(Collectors.toList());
        }

        final List<EntitySeverity> results = sortedEntityOids.stream()
            .skip(skipCount)
            .limit(paginationParameters.getLimit())
            .map(oid -> generateEntitySeverity(oid, optionalCache))
            .collect(Collectors.toList());

        if ((skipCount + results.size()) < request.getEntityIdsCount()) {
            EntitySeveritiesResponse.Builder paginationResponseBuilder =
                EntitySeveritiesResponse.newBuilder()
                    .setPaginationResponse(PaginationResponse.newBuilder());
            // if there are more results left.
            paginationResponseBuilder.getPaginationResponseBuilder()
                .setNextCursor(String.valueOf(skipCount + paginationParameters.getLimit()));
            responseObserver.onNext(paginationResponseBuilder.build());
        }
        Iterators.partition(results.iterator(), maxAmountOfEntitiesPerGrpcMessage)
            .forEachRemaining(chunk -> {
                EntitySeveritiesResponse.Builder responseBuilder = EntitySeveritiesResponse.newBuilder();
                responseBuilder
                    .setEntitySeverity(EntitySeveritiesChunk
                        .newBuilder()
                        .addAllEntitySeverity(chunk)
                        .build());
                responseObserver.onNext(responseBuilder.build());
            });
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

        final Optional<EntitySeverityCache.SeverityCount> severityCountOptional = optionalCache.flatMap(entitySeverityCache -> entitySeverityCache.getSeverityBreakdown(oid));
        if (severityCountOptional.isPresent()) {
            for (Map.Entry<Severity, Integer> entry : severityCountOptional.get().getSeverityCounts()) {
                entitySeverityBuilder.putSeverityBreakdown(entry.getKey().getNumber(), entry.getValue());
            }
        }

        return entitySeverityBuilder.build();
    }

    private PaginationParameters resetPaginationWithDefaultLimit(
        @Nonnull final MultiEntityRequest request) {
        if (!request.getPaginationParams().hasLimit()) {
            logger.info("Search pagination with order by severity not provider a limit number, " +
                "set to default limit: " + entitySeverityDefaultPaginationLimit);
            return PaginationParameters.newBuilder(request.getPaginationParams())
                .setLimit(entitySeverityDefaultPaginationLimit)
                .build();
        }
        if (request.getPaginationParams().getLimit() > entitySeverityDefaultPaginationMax) {
            logger.info("Search pagination with order by severity limit exceed default max limit," +
                " set it to default max limit number: " + entitySeverityDefaultPaginationMax);
            return PaginationParameters.newBuilder(request.getPaginationParams())
                .setLimit(entitySeverityDefaultPaginationMax)
                .build();
        }
        return request.getPaginationParams();
    }
}
