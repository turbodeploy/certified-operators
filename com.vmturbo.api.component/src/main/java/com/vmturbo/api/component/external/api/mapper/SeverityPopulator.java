package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;

/**
 * Severity populator is a class able to retrieve severity information for specified entities
 * from severity service and populate {@link ServiceEntityApiDTO} with severities from the data.
 */
public class SeverityPopulator {
    private static final Severity NORMAL_SEVERITY = Severity.NORMAL;
    private static final Logger logger = LogManager.getLogger();
    private final EntitySeverityServiceStub severityService;

    /**
     * Constructs severity populator working through gRPC severity service.
     *
     * @param severityService severity service to use
     */
    public SeverityPopulator(@Nonnull final EntitySeverityServiceStub severityService) {
        this.severityService = severityService;
    }

    /**
     * Populate the severity for a list of service entities.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable for any reason, severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityDtos A collection of {@link ServiceEntityApiDTO} whose severity should be updated.
     * @throws InterruptedException if current thread has been interrupted while pupulating
     *         severity data
     * @throws ConversionException if error faced during severity population
     */
    public void populate(final long topologyContextId,
            @Nonnull final Collection<ServiceEntityApiDTO> entityDtos)
            throws InterruptedException, ConversionException {
        final Map<Long, ServiceEntityApiDTO> entities = entityDtos.stream()
                .collect(Collectors.toMap(entity -> Long.parseLong(entity.getUuid()),
                        Function.identity()));
        final Future<SeverityMap> severityMapFuture =
                getSeverityMap(topologyContextId, entities.keySet());
        final SeverityMap severityMap;
        try {
            severityMap = severityMapFuture.get(600, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new ConversionException(
                    "Failed to retrieve severity data for entities " + entities.keySet() + ": " +
                            e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new ConversionException("Timed out while retrieving severity data");
        }
        for (Entry<Long, ServiceEntityApiDTO> entry: entities.entrySet()) {
            final long oid = entry.getKey();
            final ServiceEntityApiDTO entity = entry.getValue();
            entity.setSeverity(ActionDTOUtil.getSeverityName(severityMap.getSeverity(oid)));
        }
    }


    /**
     * Calculate the severities for the given entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return a mapping between entity id and corresponding severity.
     */
    @Nonnull
    private Future<SeverityMap> calculateSeverities(final long topologyContextId,
                                               @Nonnull final Collection<Long> entityOids) {
        final SeverityObserver observer = new SeverityObserver(entityOids.size());
        severityService.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(entityOids)
                .build(), observer);
        return observer.future;
    }

    /**
     * Gets the severity map for the specified context and collection of entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    public Future<SeverityMap> getSeverityMap(final long topologyContextId,
            @Nonnull final Collection<Long> entityOids) {
        return calculateSeverities(topologyContextId, entityOids);
    }

    /**
     * Local helper class used to store severity data retrieved from severity service.
     */
    private static class SeverityObserver implements StreamObserver<EntitySeveritiesResponse> {
        private final Map<Long, Severity> resultMap;
        private final CompletableFuture<SeverityMap> future;

        SeverityObserver(int expectedSize) {
            this.resultMap = new HashMap<>(expectedSize);
            this.future = new CompletableFuture<>();
        }

        @Override
        public void onNext(EntitySeveritiesResponse value) {
            for (EntitySeverity severity : value.getEntitySeverity().getEntitySeverityList()) {
                if (resultMap.put(severity.getEntityId(), severity.getSeverity()) != null) {
                    logger.warn("Received multiple records from severity service for entity {}",
                            severity.getEntityId());
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            future.complete(new SeverityMapImpl(Collections.unmodifiableMap(resultMap)));
        }
    }

    /**
     * Wraps a map of EntityId -> Severity.
     * Rather than returning null for an unknown entity, it returns NORMAL.
     */
    public static class SeverityMapImpl implements SeverityMap {
        private final Map<Long, Severity> severities;

        private SeverityMapImpl(@Nonnull final Map<Long, Severity> severities) {
            this.severities = Objects.requireNonNull(severities);
        }

        /**
         * Return the severity for an entity by ID. If the entity is unknown, return NORMAL.
         *
         * @param id The id of the entity whose severity should be retrieved..
         * @return the severity for an entity by ID. If the entity is unknown, return NORMAL.
         */
        @Nonnull
        public Severity getSeverity(@Nonnull final Long id) {
            final Severity existing = severities.get(id);
            if (existing == null) {
                return NORMAL_SEVERITY;
            } else {
                return existing;
            }
        }

        /**
         * Calculate the highest severity for the passed in entity OIDs.
         *
         * @param entityOids The set of entity OIDs.
         * @return calculated highest severity
         */
        @Nonnull
        public Severity calculateSeverity(@Nonnull final Collection<Long> entityOids) {
            return entityOids.stream()
                    .map(severities::get)
                    .filter(Objects::nonNull)
                    .max(Comparator.comparing(Severity::getNumber))
                    .orElse(NORMAL_SEVERITY);
        }
    }
}
