package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
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
        final SeverityMapImpl severityMap;
        try {
            final SeverityMap map = severityMapFuture.get(600, TimeUnit.SECONDS);
            if (map instanceof SeverityMapImpl) {
                severityMap = (SeverityMapImpl)map;
            } else {
                throw new ConversionException("Wrong severity data format, " +
                        "expecting severity breakdown property");
            }
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
            entity.setSeverityBreakdown(severityMap.getSeverityBreakdown(oid));
        }
    }

    /**
     * Populate the severities for all entities in the SupplyChain. If the action orchestrator cannot
     * find a severity for an entity in the SupplyChain, its Severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable any reason, the severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param supplychainApiDTO The supply chain whose entities should be populated.
     */
    public void populate(final long topologyContextId,
                         @Nonnull final SupplychainApiDTO supplychainApiDTO)
            throws InterruptedException, ConversionException{
        final Set<Long> supplyChainEntityIds = supplychainApiDTO.getSeMap().values().stream()
                .filter(Objects::nonNull)
                .flatMap(supplychainEntryDTO ->
                        supplychainEntryDTO.getInstances().keySet().stream().map(Long::parseLong))
                .collect(Collectors.toSet());

        final SeverityMapImpl severityMap;
        try {
            final SeverityMap map = calculateSeverities(
                    topologyContextId, supplyChainEntityIds).get(600, TimeUnit.SECONDS);
            if (map instanceof SeverityMapImpl) {
                severityMap = (SeverityMapImpl)map;
            } else {
                throw new ConversionException("Wrong severity data format, " +
                        "expecting severity breakdown property");
            }
        } catch (ExecutionException e) {
            throw new ConversionException(
                    "Failed to retrieve severity data for supply chain " + supplychainApiDTO.getDisplayName() + ": " +
                            e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new ConversionException("Timed out while retrieving severity data");
        }

        supplychainApiDTO.getSeMap().forEach((entityType, supplychainEntryDTO) -> {
            if (supplychainEntryDTO != null) {
                supplychainEntryDTO.getInstances().forEach((uuid, serviceEntityDTO) -> {
                    long oid = Long.parseLong(uuid);
                    serviceEntityDTO.setSeverity(ActionDTOUtil.getSeverityName(severityMap.getSeverity(oid)));
                    serviceEntityDTO.setSeverityBreakdown(severityMap.getSeverityBreakdown(oid));
                });
            }
        });
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
        private final Map<Long, EntitySeverity> resultMap;
        private final CompletableFuture<SeverityMap> future;

        SeverityObserver(int expectedSize) {
            this.resultMap = new HashMap<>(expectedSize);
            this.future = new CompletableFuture<>();
        }

        @Override
        public void onNext(EntitySeveritiesResponse value) {
            for (EntitySeverity severity : value.getEntitySeverity().getEntitySeverityList()) {
                if (resultMap.put(severity.getEntityId(), severity) != null) {
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
        private final Map<Long, EntitySeverity> severities;

        private SeverityMapImpl(@Nonnull final Map<Long, EntitySeverity> severities) {
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
            return getAllEntitySeverities(severities.get(id)).stream()
                    .filter(Objects::nonNull)
                    .max(Comparator.comparing(Severity::getNumber))
                    .orElse(Severity.NORMAL);
        }

        /**
         * Calculate the highest severity for the passed in entity OIDs using each entity level
         * severity, and the severity breakdown.
         *
         * @param entityOids The set of entity OIDs.
         * @return calculated highest severity
         */
        @Nonnull
        public Severity calculateSeverity(@Nonnull final Collection<Long> entityOids) {
            return entityOids.stream()
                    .map(severities::get)
                    .filter(Objects::nonNull)
                    .map(this::getAllEntitySeverities)
                    .flatMap(Collection::stream)
                    .filter(Objects::nonNull)
                    .max(Comparator.comparing(Severity::getNumber))
                    .orElse(NORMAL_SEVERITY);
        }

        /**
         * Expands all severities from entity level severity and severity breaks with a count
         * greater than 0.
         *
         * @param entitySeverity the entity severity to expand.
         *
         * @return all severities from entity level severity and severity breaks with a count
         *            greater than 0. Returns an empty list if there is no entity severity and
         *            no severity breakdown with a count greater than 0.
         */
        @Nonnull
        private List<Severity> getAllEntitySeverities(@Nullable EntitySeverity entitySeverity) {
            List<Severity> result = new ArrayList<>();
            if (entitySeverity == null) {
                return result;
            }
            if (entitySeverity.getSeverityBreakdownMap() != null) {
                for (Map.Entry<Integer, Long> entry : entitySeverity.getSeverityBreakdownMap().entrySet()) {
                    if (entry.getKey() != null
                            && entry.getValue() != null
                            && entry.getValue() > 0
                            && entry.getKey() >= 0
                            && Severity.forNumber(entry.getKey()) != null) {
                        result.add(Severity.forNumber(entry.getKey()));
                    }
                }
            }
            if (entitySeverity.getSeverity() != null) {
                result.add(entitySeverity.getSeverity());
            }
            return result;
        }

        /**
         * Return the severity breakdown for the given id. Returns null if not found.
         *
         * @param id the id to search for.
         * @return the severity breakdown for the given id. Returns null if not found.
         */
        @Nullable
        public Map<String, Long> getSeverityBreakdown(long id) {
            EntitySeverity entitySeverity = severities.get(id);
            if (entitySeverity == null) {
                return null;
            }
            return entitySeverity.getSeverityBreakdownMap().entrySet().stream()
                    .filter(entry -> entry.getKey() != null)
                    .collect(Collectors.toMap(
                            entry -> Severity.forNumber(entry.getKey()).name(),
                            Entry::getValue));
        }
    }
}
