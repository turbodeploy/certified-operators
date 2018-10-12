package com.vmturbo.market.topology.conversions;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.market.topology.conversions.CostLibrary.ComputePriceBundle;
import com.vmturbo.market.topology.conversions.CostLibrary.ComputePriceBundle.ComputePrice;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.CostTuple;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.builders.BusinessAccountBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class CostDTOCreator {
    private static final Logger logger = LogManager.getLogger();
    CostLibrary costLibrary;
    CommodityConverter commodityConverter;

    public CostDTOCreator(CommodityConverter commodityConverter, CostLibrary costLibrary) {
        this.commodityConverter = commodityConverter;
        this.costLibrary = costLibrary;
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier the compute tier topology entity DTO
     * @param region the region topology entity DTO
     * @param businessAccountDTOs all business accounts in the topology
     *
     * @return CostDTO
     */
    public CostDTO createComputeTierCostDTO(TopologyEntityDTO tier, TopologyEntityDTO region, Set<TopologyEntityDTO> businessAccountDTOs) {
        ComputeTierCostDTO.Builder computeTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        //createComputeResourceDependency(tier); TODO: add it once dto has proper field to store
        ComputePriceBundle priceBundle = costLibrary.getComputePriceBundle(tier.getOid(), region.getOid());
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (ComputePrice price : priceBundle.getPrices()) {
            List<CommodityType> licenseCommType = licenseCommoditySet.stream()
                    .filter(c -> c.getKey().equals(price.getOsType().name())).collect(Collectors.toList());
            if (licenseCommType.size() != 1) {
                logger.warn("Entity in tier {} region {} does not have wrong number of license",
                        tier.getDisplayName(), region.getDisplayName());
                continue;
            }
            if (!baOidSet.contains(price.getAccountId())) {
                logger.warn("Entity in tier {} region {} does not have business account oid {},"
                        + " yet the account is found in cost component",
                        tier.getDisplayName(), region.getDisplayName(), price.getAccountId());
                continue;
            }
            for (long oid : baOidSet) {
                computeTierDTOBuilder
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(oid)
                                .setLicenseCommodityType(commodityConverter
                                        .toMarketCommodityId(licenseCommType.get(0)))
                                .setPrice(price.getHourlyPrice())
                                .build());
            }
        }

        return CostDTO.newBuilder()
                .setComputeTierCost(computeTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .build())
                .build();
    }

    /**
     * Create CostDTO for a given storage tier and region based traderDTO.
     *
     * @param tier the storage tier topology entity DTO
     * @param region the region topology entity DTO
     * @param businessAccountDTOs all business accounts in the topology
     *
     * @return CostDTO
     */
    public CostDTO createStorageTierCostDTO(TopologyEntityDTO tier, TopologyEntityDTO region, Set<TopologyEntityDTO> businessAccountDTOs) {
        return CostDTO.newBuilder()
                .setStorageTierCost(StorageTierCostDTO.newBuilder().build()).build();
    }

    public ComputeResourceDependency createComputeResourceDependency(TopologyEntityDTO tier) {
        return null;
    }
}