package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;

public class SupplyChainTestUtils {
    public SupplyChainTestUtils() {
    }

    /**
     * Create a mock {@link SupplychainApiDTOFetcherBuilder} and attach it to the input
     * {@link SupplyChainFetcherFactory}.
     *
     * @param factoryMock The factory. This should be a mockito mock.
     * @return The {@link SupplychainApiDTOFetcherBuilder}. This will also be returned by
     *         the input factory's {@link SupplyChainFetcherFactory#newApiDtoFetcher()}
     */
    @Nonnull
    public SupplychainApiDTOFetcherBuilder mockApiDtoBuilder(SupplyChainFetcherFactory factoryMock) {
        SupplychainApiDTOFetcherBuilder supplyChainFetcherOperationBuilderMock =
            mock(SupplychainApiDTOFetcherBuilder.class);
        when(supplyChainFetcherOperationBuilderMock.entityTypes(anyListOf(String.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.entityStates(anyListOf(EntityState.class)))
                .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.topologyContextId(anyLong()))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.addSeedUuids(anySetOf(String.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.apiEnvironmentType(any(EnvironmentType.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.entityDetailType(any(EntityDetailType.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.aspectsToInclude(any(Collection.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.entityAspectMapper(any(EntityAspectMapper.class)))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(supplyChainFetcherOperationBuilderMock.includeHealthSummary(anyBoolean()))
            .thenReturn(supplyChainFetcherOperationBuilderMock);
        when(factoryMock.newApiDtoFetcher()).thenReturn(supplyChainFetcherOperationBuilderMock);
        return supplyChainFetcherOperationBuilderMock;
    }

    public ServiceEntityApiDTO createServiceEntityApiDTO(long id) {
        return createServiceEntityApiDTO(id, "PhysicalMachine");
    }

    public ServiceEntityApiDTO createServiceEntityApiDTO(long id, String entityType) {
        ServiceEntityApiDTO answer = new ServiceEntityApiDTO();
        answer.setUuid(Long.toString(id));
        answer.setClassName(entityType);
        return answer;
    }

    public ServiceEntityApiDTO createServiceEntityApiDTO(long id, long targetId) {
        ServiceEntityApiDTO se = createServiceEntityApiDTO(id);
        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(String.valueOf(targetId));
        se.setDiscoveredBy(targetApiDTO);
        return se;
    }

    public SupplychainEntryDTO createSupplyChainEntryDTO(
            @Nonnull final String entityType,
            @Nonnull final Long... instanceIds) {
        SupplychainEntryDTO answer = new SupplychainEntryDTO();
        answer.setEntitiesCount(instanceIds.length);
        // for each ID, create a map entry "id" -> SE(id)
        answer.setInstances(Arrays.stream(instanceIds)
                .collect(Collectors.toMap(
                        id -> Long.toString(id),
                        id -> createServiceEntityApiDTO(id, entityType))));
        return answer;
    }

    SupplychainApiDTO createSupplychainApiDTO() {
        SupplychainApiDTO answer = new SupplychainApiDTO();
        Map<String, SupplychainEntryDTO> seMap = new HashMap<>();
        answer.setSeMap(seMap);
        return answer;
    }


    /**
     * Populate the {@link SupplychainEntryDTO} with the given OID -> severity values.
     * Both the individual ServiceEntities are updated as well as the healthSummary map
     * in the SupplyChainEntryDTO which includes counts for each severity type.
     * SE's not in this map are not changed, nor counted in the healthSummary.
     *
     * @param supplyChainEntryDTO the {@link SupplychainEntryDTO} to be populated
     * @param healthValues the map of OID -> severity for each ServiceEntity to be updated.
     */
    public void addHealthSummary(SupplychainEntryDTO supplyChainEntryDTO,
                                 ImmutableMap<Long, String> healthValues) {
        Map<String, ServiceEntityApiDTO> instanceMap = supplyChainEntryDTO.getInstances();
        Map<String, Integer> healthSummary = Maps.newHashMap();
        supplyChainEntryDTO.setHealthSummary(healthSummary);
        healthValues.forEach((oid, severity) -> {
            instanceMap.get(Long.toString(oid)).setSeverity(severity);
            healthSummary.put(severity, healthSummary.getOrDefault(severity, 0) + 1);
        });

    }
}
