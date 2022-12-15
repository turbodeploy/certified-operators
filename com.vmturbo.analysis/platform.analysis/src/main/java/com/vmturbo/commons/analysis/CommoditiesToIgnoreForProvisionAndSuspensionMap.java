package com.vmturbo.commons.analysis;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The commodities that will not be considered for provision and suspension logic.
 */
public class CommoditiesToIgnoreForProvisionAndSuspensionMap {

    /**
     * Commodities that should be ignored while computing the expense for suspension and provision.
     * The key is the entity type and the values are the set of commodity basetypes.
     */
    private final Map<Integer, Set<Integer>>
            commoditiesToIgnoreForProvisionAndSuspensionMap =
            new ImmutableMap.Builder<Integer, Set<Integer>>().put(EntityType.PHYSICAL_MACHINE_VALUE,
                    ImmutableSet.of(CommodityType.CPU_PROVISIONED_VALUE,
                            CommodityType.MEM_PROVISIONED_VALUE)).put(EntityType.CLUSTER_VALUE,
                    ImmutableSet.of(CommodityType.CPU_PROVISIONED_VALUE,
                            CommodityType.MEM_PROVISIONED_VALUE)).build();

    public Map<Integer, Set<Integer>> getCommoditiesToIgnoreForProvisionAndSuspensionMap() {
        return commoditiesToIgnoreForProvisionAndSuspensionMap;
    }
}
