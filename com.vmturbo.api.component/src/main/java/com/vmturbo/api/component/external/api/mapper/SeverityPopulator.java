package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Streams;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse.TypeCase;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;

public class SeverityPopulator {
    private static final Logger logger = LogManager.getLogger();
    private final EntitySeverityServiceBlockingStub severityService;

    public SeverityPopulator(@Nonnull final EntitySeverityServiceBlockingStub severityService) {
        this.severityService = severityService;
    }

    /**
     * Populate the severity for a single service entity.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable for any reason, severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entity The DTO whose severity should be updated.
     * @return The updated DTO.
     */
    @Nonnull
    public ServiceEntityApiDTO populate(final long topologyContextId,
                                        @Nonnull final ServiceEntityApiDTO entity) {
        return populate(topologyContextId, Collections.singleton(entity)).iterator().next();
    }

    /**
     * Populate the severity for a list of service entities.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable for any reason, severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityDtos A collection of {@link ServiceEntityApiDTO} whose severity should be updated.
     * @return The updated DTOs.
     */
    @Nonnull
    public Collection<ServiceEntityApiDTO> populate(final long topologyContextId,
        @Nonnull final Collection<ServiceEntityApiDTO> entityDtos) {
        final SeverityMap severityMap = new SeverityMap(calculateSeverities(
            topologyContextId,
            entityDtos.stream()
                .map(ServiceEntityApiDTO::getUuid)
                .map(Long::parseLong)
                .collect(Collectors.toList())));
        for (ServiceEntityApiDTO entityDto : entityDtos) {
            long entityOid = Long.parseLong(entityDto.getUuid());
            entityDto.setSeverity(severityMap.getSeverity(entityOid));
            entityDto.setSeverityBreakdown(severityMap.getSeverityBreakdown(entityOid));
        }
        return entityDtos;
    }

    /**
     * Populate the severities for all entities in the SupplyChain. If the action orchestrator cannot
     * find a severity for an entity in the SupplyChain, its Severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable any reason, the severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param supplychainApiDTO The supply chain whose entities should be populated.
     * @return The input supply chain with populated severities.
     */
    @Nonnull
    public SupplychainApiDTO populate(final long topologyContextId,
                                      @Nonnull final SupplychainApiDTO supplychainApiDTO) {
        final Set<Long> supplyChainEntityIds = supplychainApiDTO.getSeMap().values().stream()
            .filter(Objects::nonNull)
            .flatMap(supplychainEntryDTO ->
                supplychainEntryDTO.getInstances().keySet().stream().map(Long::parseLong))
            .collect(Collectors.toSet());

        final SeverityMap severityMap = new SeverityMap(calculateSeverities(
            topologyContextId, supplyChainEntityIds));

        supplychainApiDTO.getSeMap().forEach((entityType, supplychainEntryDTO) -> {
            if (supplychainEntryDTO != null) {
                supplychainEntryDTO.getInstances().forEach((uuid, serviceEntityDTO) -> {
                    long oid = Long.parseLong(uuid);
                    serviceEntityDTO.setSeverity(severityMap.getSeverity(oid));
                    serviceEntityDTO.setSeverityBreakdown(severityMap.getSeverityBreakdown(oid));
                });
            }
        });
        return supplychainApiDTO;
    }

    /**
     * Calculate the severities for the given entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return a mapping between entity id and corresponding severity.
     */
    @Nonnull
    private Map<Long, EntitySeverity> calculateSeverities(final long topologyContextId,
                                                          @Nonnull final Collection<Long> entityOids) {
        try {
            Iterator<EntitySeveritiesResponse> response = severityService.getEntitySeverities(
                MultiEntityRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .addAllEntityIds(entityOids)
                    .build());

            return Streams.stream(response)
                .filter(chunk -> chunk.getTypeCase() == TypeCase.ENTITY_SEVERITY)
                .map(EntitySeveritiesResponse::getEntitySeverity)
                .flatMap(chunk -> chunk.getEntitySeverityList().stream())
                .collect(Collectors.toMap(EntitySeverity::getEntityId, entitySeverity -> entitySeverity));
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                // This is a gRPC StatusRuntimeException
                Status status = ((StatusRuntimeException)e).getStatus();
                logger.warn("Unable to fetch severities: {} caused by {}.",
                    status.getDescription(), status.getCause());
            } else {
                logger.error("Error when fetching severities: ", e);
            }
        }
        return Collections.emptyMap();
}


    /**
     * Gets the severity map for the specified context and collection of entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    public SeverityMap getSeverityMap(final long topologyContextId,
            @Nonnull final Collection<Long> entityOids) {
        return new SeverityMap(calculateSeverities(topologyContextId, entityOids));
    }

    /**
     * Populate a map of entity ids to their corresponding optional ServiceEntityApiDTOs.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable, the severity will be set to NORMAL.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityDTOs A map of ids -> corresponding ServiceEntityApiDTO whose severities should be populated.
     * @return The input DTOs.
     */
    @Nonnull
    public Map<Long, ServiceEntityApiDTO> populate(final long topologyContextId,
                                                   @Nonnull final Map<Long, ServiceEntityApiDTO> entityDTOs) {
        final SeverityMap severityMap = new SeverityMap(calculateSeverities(
            topologyContextId, entityDTOs.keySet()));
        entityDTOs.forEach((entityId, dto) -> {
            dto.setSeverity(severityMap.getSeverity(entityId));
            dto.setSeverityBreakdown(severityMap.getSeverityBreakdown(entityId));
        });
        return entityDTOs;
    }

    /**
     * Wraps a map of EntityId -> Severity.
     * Rather than returning null for an unknown entity, it returns NORMAL.
     */
    public static class SeverityMap {
        private final Map<Long, EntitySeverity> severities;

        /**
         * Constructs the wrapper class containing severity and severity breakdown mappings.
         *
         * @param severities the severities to translate missing entries to normal.
         */
        private SeverityMap(@Nonnull final Map<Long, EntitySeverity> severities) {
            this.severities = Objects.requireNonNull(severities);
        }

        /**
         * Return the severity for an entity by ID. If the entity is unknown, return NORMAL.
         *
         * @param id The id of the entity whose severity should be retrieved..
         * @return the severity for an entity by ID. If the entity is unknown, return NORMAL.
         */
        @Nonnull
        public String getSeverity(@Nonnull final Long id) {
            return ActionDTOUtil.getSeverityName(
                getAllEntitySeverities(severities.get(id)).stream()
                    .filter(Objects::nonNull)
                    .max(Comparator.comparing(Severity::getNumber))
                    .orElse(Severity.NORMAL));
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
                    .orElse(Severity.NORMAL);
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
