package com.vmturbo.market.diagnostics;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * An instance of this object represents a utilization distribution of a commodity sold by an entity type.
 * For ex., 1 -> 90
 *          2 -> 95
 *          40 -> 55
 * This means there are 90 entities with 1% util, 95 entities with 2% util and 40 entities with 55% util.
 */
class UtilizationDistribution {

    private static final Map<Integer, Set<Integer>> commTypesToPlotDistributionByEntityType = ImmutableMap.of(
        EntityType.STORAGE_VALUE, ImmutableSet.of(
            CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
            CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
            CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE,
            CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE),
        EntityType.PHYSICAL_MACHINE_VALUE, ImmutableSet.of(
            CommodityDTO.CommodityType.CPU_VALUE,
            CommodityDTO.CommodityType.MEM_VALUE
        ));

    private final Map<Integer, Integer> utilDistribution = Maps.newTreeMap();

    /**
     * Create the utilization distribution for the {@code traders}.
     * @param traders the source or projected traders
     * @return Mapping from Entity type to commodity string to Utilization distribution for that comm sold.
     */
    static Map<Integer, Map<String, UtilizationDistribution>> createUtilizationDistribution(
        Iterable<TraderTO> traders) {
        // Mapping from Entity type to commodity string to Utilization distribution
        Map<Integer, Map<String, UtilizationDistribution>> commUtilDistributionsByEntityType = Maps.newHashMap();
        for (TraderTO projectedTrader : traders) {
            int entityType = projectedTrader.getType();
            Set<Integer> commTypesForDistribution = commTypesToPlotDistributionByEntityType.get(entityType);
            if (commTypesForDistribution != null) {
                for (CommoditySoldTO commSold : projectedTrader.getCommoditiesSoldList()) {
                    CommoditySpecificationTO commSpec = commSold.getSpecification();
                    if (commTypesForDistribution.contains(commSpec.getBaseType())) {
                        Map<String, UtilizationDistribution> utilDistributionByCommType =
                            commUtilDistributionsByEntityType.computeIfAbsent(entityType, type -> Maps.newHashMap());
                        UtilizationDistribution distribution = utilDistributionByCommType.computeIfAbsent(
                            commSpec.getDebugInfoNeverUseInCode() + "_" + commSpec.getType(), s -> new UtilizationDistribution());
                        distribution.addDataPoint(commSold);
                    }
                }
            }
        }
        return commUtilDistributionsByEntityType;
    }

    private void addDataPoint(CommoditySoldTO commSold) {
        if (commSold.getCapacity() == 0) {
            return;
        }
        int util = Math.round(commSold.getQuantity() * 100 / commSold.getCapacity());
        Integer numOfEntities = utilDistribution.get(util);
        if (numOfEntities == null) {
            utilDistribution.put(util, 1);
        } else {
            utilDistribution.put(util, ++numOfEntities);
        }
    }

    /**
     * Get the utilization distribution represented by this object.
     * @return the utilization distribution represented by this object.
     */
    Map<Integer, Integer> getUnmodifiableUtilDistribution() {
        return Collections.unmodifiableMap(utilDistribution);
    }
}
