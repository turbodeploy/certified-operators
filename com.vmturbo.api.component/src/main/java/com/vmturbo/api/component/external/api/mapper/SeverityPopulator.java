package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;
import com.vmturbo.common.protobuf.severity.SeverityMap;
import com.vmturbo.common.protobuf.severity.SeverityMapper;
import com.vmturbo.common.protobuf.severity.SeverityUtil;

/**
 * Severity populator is a class able to retrieve severity information for specified entities
 * from severity service and populate {@link ServiceEntityApiDTO} with severities from the data.
 */
public class SeverityPopulator {

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
     * @throws InterruptedException if current thread has been interrupted while populating
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
        final SeverityMapper severityMap;
        try {
            final SeverityMap map = severityMapFuture.get(600, TimeUnit.SECONDS);
            if (map instanceof SeverityMapper) {
                severityMap = (SeverityMapper)map;
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

        final SeverityMapper severityMap;
        try {
            final SeverityMap map = SeverityUtil.calculateSeverities(topologyContextId,
                    supplyChainEntityIds, severityService).get(600, TimeUnit.SECONDS);
            if (map instanceof SeverityMapper) {
                severityMap = (SeverityMapper)map;
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
     * Gets the severity map for the specified context and collection of entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    public Future<SeverityMap> getSeverityMap(final long topologyContextId,
                                              @Nonnull final Collection<Long> entityOids) {
        return SeverityUtil.calculateSeverities(topologyContextId, entityOids, severityService);
    }
}
