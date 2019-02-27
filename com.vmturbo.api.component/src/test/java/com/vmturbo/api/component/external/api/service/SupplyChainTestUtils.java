package com.vmturbo.api.component.external.api.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;

public class SupplyChainTestUtils {
    public SupplyChainTestUtils() {
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
