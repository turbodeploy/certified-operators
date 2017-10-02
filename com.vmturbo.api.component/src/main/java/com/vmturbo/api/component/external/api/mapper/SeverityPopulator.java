package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.dto.SupplychainApiDTO;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;

public class SeverityPopulator {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Populate the severity for a single service entity.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable, severity will not be populated.
     *
     * @param entitySeverityRpc The entity severity service that will provide severity information
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityDtos The DTOs whose severity should be updated.
     * @return The updated DTOs.
     */
    @Nonnull
    public static Collection<ServiceEntityApiDTO> populate(
        @Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
        @Nonnull final long topologyContextId,
        @Nonnull final Collection<ServiceEntityApiDTO> entityDtos) {
        try {
            final SeverityMap severityMap = new SeverityMap(getSeverities(entitySeverityRpc,
                entityDtos.stream().map(ServiceEntityApiDTO::getUuid).map(Long::parseLong).collect(Collectors.toList()),
                topologyContextId));
            entityDtos.forEach(entityDto -> entityDto.setSeverity(
                severityMap.getSeverity(Long.parseLong(entityDto.getUuid()))));
        } catch (RuntimeException e) {
            logger.error("Error requesting severity from action orchestrator: {}", e);
        }

        return entityDtos;
    }

    /**
     * Populate the severities for all entities in the SupplyChain. If the action orchestrator cannot
     * find a severity for an entity in the SupplyChain, its Severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable, the severity will not be populated.
     *
     * @param entitySeverityRpc The action orchestrator to use to retrieve severities.
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param supplychainApiDTO The supply chain whose entities should be populated.
     * @return The input supply chain with populated severities.
     */
    @Nonnull
    public static SupplychainApiDTO populate(@Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
                                             final long topologyContextId,
                                             @Nonnull final SupplychainApiDTO supplychainApiDTO) {
        try {
            final Set<Long> supplyChainEntityIds = supplychainApiDTO.getSeMap().values().stream()
                .filter(Objects::nonNull)
                .flatMap(supplychainEntryDTO ->
                    supplychainEntryDTO.getInstances().keySet().stream().map(Long::parseLong))
                .collect(Collectors.toSet());

            final SeverityMap severityMap = new SeverityMap(getSeverities(entitySeverityRpc,
                supplyChainEntityIds, topologyContextId));

            supplychainApiDTO.getSeMap().forEach((entityType, supplychainEntryDTO) -> {
                if (supplychainEntryDTO != null) {
                    supplychainEntryDTO.getInstances().forEach((id, serviceEntityDTO) -> {
                        serviceEntityDTO.setSeverity(severityMap.getSeverity(Long.parseLong(id)));
                    });
                }
            });
        } catch (RuntimeException e) {
            logger.error("Error requesting severity from action orchestrator: {}", e);
        }

        return supplychainApiDTO;
    }

    /**
     * Populate a map of entity ids to their corresponding optional ServiceEntityApiDTOs.
     * If the AO has no action information for an entity, its severity will be set to NORMAL.
     * If the ActionOrchestrator is unreachable, the severity will not be populated.
     *
     * @param entitySeverityRpc The entity severity service to use to retrieve severities.
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityDTOs A map of ids -> corresponding ServiceEntityApiDTO whose severities should be populated.
     * @return The input DTOs.
     */
    @Nonnull
    public static Map<Long, Optional<ServiceEntityApiDTO>> populate(@Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
                                                                    final long topologyContextId,
                                                                    @Nonnull final Map<Long, Optional<ServiceEntityApiDTO>> entityDTOs) {
        try {
            final SeverityMap severityMap = new SeverityMap(getSeverities(entitySeverityRpc,
                entityDTOs.keySet(), topologyContextId));
            entityDTOs.forEach((entityId, optionalDTO) ->
                optionalDTO.ifPresent(dto -> dto.setSeverity(severityMap.getSeverity(entityId))));
        } catch (RuntimeException e) {
            logger.error("Error requesting severity from action orchestrator: {}", e);
        }

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

    private static Map<Long, Optional<Severity>> getSeverities(@Nonnull final EntitySeverityServiceBlockingStub entitySeverityRpc,
                                                               @Nonnull final Collection<Long> entityIds,
                                                               final long topologyContextId)
    {
        Iterable<EntitySeverity> severitiesIterable = () -> entitySeverityRpc.getEntitySeverities(
            MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(entityIds)
                .build());

        return StreamSupport.stream(severitiesIterable.spliterator(), false)
            .collect(Collectors.toMap(
                EntitySeverity::getEntityId,
                entitySeverity -> entitySeverity.hasSeverity() ? Optional.of(entitySeverity.getSeverity()) : Optional.empty()
            ));
    }
}
