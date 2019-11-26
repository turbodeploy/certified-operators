package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
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
        entityDtos.forEach(entityDto -> entityDto.setSeverity(
            severityMap.getSeverity(Long.parseLong(entityDto.getUuid()))));
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
                supplychainEntryDTO.getInstances().forEach((id, serviceEntityDTO) -> {
                    serviceEntityDTO.setSeverity(severityMap.getSeverity(Long.parseLong(id)));
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
    public Map<Long, Optional<Severity>> calculateSeverities(final long topologyContextId,
                                                @Nonnull final Collection<Long> entityOids) {
        try {
            return severityService.getEntitySeverities(
                MultiEntityRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .addAllEntityIds(entityOids)
                    .build())
                .getEntitySeverityList().stream()
                .collect(Collectors.toMap(EntitySeverity::getEntityId,
                    entitySeverity -> entitySeverity.hasSeverity() ?
                        Optional.of(entitySeverity.getSeverity()) : Optional.empty()));
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
        return entityOids.stream()
                .collect(Collectors.toMap(Function.identity(),
                        id -> Optional.of(Severity.NORMAL)));
    }

    /**
     * Calculate the highest severity for the passed in entity OIDs.
     * TODO: to improve performance, move the calculation to Group component, and cache results.
     * TODO: Created OM-43416 for this change.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    public Optional<Severity> calculateSeverity(final long topologyContextId,
                                                @Nonnull final Collection<Long> entityOids) {
        try {
            List<EntitySeverity> severitiesList = severityService.getEntitySeverities(
                    MultiEntityRequest.newBuilder()
                            .setTopologyContextId(topologyContextId)
                            .addAllEntityIds(entityOids)
                            .build())
                    .getEntitySeverityList();

            // Calculate the highest severity based on enum value: NORMAL(1), MINOR(2), MAJOR(3), CRITICAL(4)
            return Optional.ofNullable(severitiesList
                    .stream()
                    .map(EntitySeverity::getSeverity)
                    .reduce(Severity.NORMAL, (first, second)
                            -> first.getNumber() > second.getNumber() ? first : second));
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
        return Optional.of(Severity.NORMAL);
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
        entityDTOs.forEach((entityId, dto) -> dto.setSeverity(severityMap.getSeverity(entityId)));
        return entityDTOs;
    }

    /**
     * Wraps a map of EntityId -> Severity.
     * Rather than returning null for an unknown entity, it returns NORMAL.
     */
    private static class SeverityMap {
        private final String NORMAL_SEVERITY_NAME = ActionDTOUtil.getSeverityName(Severity.NORMAL);
        private final Map<Long, Optional<Severity>> severities;

        public SeverityMap(@Nonnull final Map<Long, Optional<Severity>> severities) {
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
            return severities.get(id)
                .map(ActionDTOUtil::getSeverityName)
                .orElse(NORMAL_SEVERITY_NAME);
        }
    }

}
