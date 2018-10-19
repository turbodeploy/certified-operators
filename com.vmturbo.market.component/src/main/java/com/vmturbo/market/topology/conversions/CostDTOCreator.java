package com.vmturbo.market.topology.conversions;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CostDTOCreator {
    private static final Logger logger = LogManager.getLogger();
    private final MarketPriceTable marketPriceTable;
    private final CommodityConverter commodityConverter;

    public CostDTOCreator(CommodityConverter commodityConverter, MarketPriceTable marketPriceTable) {
        this.commodityConverter = commodityConverter;
        this.marketPriceTable = marketPriceTable;
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier is topology entity DTO representation of the tier
     * @param region the region topology entity DTO
     * @param businessAccountDTOs all business accounts in the topology
     *
     * @return CostDTO
     */
    public CostDTO createCostDTO(TopologyEntityDTO tier, TopologyEntityDTO region, Set<TopologyEntityDTO> businessAccountDTOs) {
        if (tier.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            return createComputeTierCostDTO(tier, region, businessAccountDTOs);
        } else {
            return createDatabaseTierCostDTO(tier, region, businessAccountDTOs);
        }
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
        ComputePriceBundle priceBundle = marketPriceTable.getComputePriceBundle(tier.getOid(), region.getOid());
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (ComputePrice price : priceBundle.getPrices()) {
            List<CommodityType> licenseCommType = licenseCommoditySet.stream()
                    .filter(c -> c.getKey().equals(price.getOsType().name())).collect(Collectors.toList());
            if (licenseCommType.size() != 1) {
                logger.warn("Entity in tier {} region {} have duplicate number of license",
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
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier the database tier topology entity DTO
     * @param region the region topology entity DTO
     * @param businessAccountDTOs all business accounts in the topology
     *
     * @return CostDTO
     */
    public CostDTO createDatabaseTierCostDTO(TopologyEntityDTO tier, TopologyEntityDTO region, Set<TopologyEntityDTO> businessAccountDTOs) {
        ComputeTierCostDTO.Builder dbTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        DatabasePriceBundle priceBundle = marketPriceTable.getDatabasePriceBundle(tier.getOid(), region.getOid());
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (DatabasePrice price : priceBundle.getPrices()) {
            if (!baOidSet.contains(price.getAccountId())) {
                logger.warn("Entity in tier {} region {} does not have business account oid {},"
                        + " yet the account is found in cost component",
                        tier.getDisplayName(), region.getDisplayName(), price.getAccountId());
                continue;
            }
            StringBuilder licenseKey = new StringBuilder();
            licenseKey.append(price.getDbEngine() != null ? price.getDbEngine().name() : "null")
                    .append(":")
                    .append(price.getDbEdition() != null ? price.getDbEdition().name() : "null")
                    .append(":")
                    .append(price.getDeploymentType() != null ? price.getDeploymentType().name() : "null")
                    .append(":")
                    .append(price.getLicenseModel() != null ? price.getLicenseModel().name() : "null");
            List<CommodityType> matchingLicenseSet = licenseCommoditySet.stream()
                    .filter(commType -> licenseKey.toString().equals(commType.getKey()))
                    .collect(Collectors.toList());

            if (matchingLicenseSet.size() == 0) {
                logger.warn("Entity in tier {} region {} does not sell license matching key {}.",
                        tier.getDisplayName(), region.getDisplayName(), licenseKey);
                continue;
            }

            if (matchingLicenseSet.size() > 1) {
                logger.warn("Entity in tier {} region {} sells multiple duplicate licenses matching key {}.",
                        tier.getDisplayName(), region.getDisplayName(), licenseKey);
                continue;
            }

            for (long oid : baOidSet) {
                dbTierDTOBuilder
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(oid)
                                .setLicenseCommodityType(commodityConverter
                                        .toMarketCommodityId(matchingLicenseSet.get(0)))
                                .setPrice(price.getHourlyPrice())
                                .build());
            }
        }

        return CostDTO.newBuilder()
                .setComputeTierCost(dbTierDTOBuilder
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

    /**
     * Create {@link ComputeResourceDependency} which specifies the constraint between comm bought used
     * and comm sold capacity.
     *
     * @param tier the compute tier topology entity DTO
     * @return ComputeResourceDependency
     */
    public ComputeResourceDependency createComputeResourceDependency(TopologyEntityDTO tier) {
        return ComputeResourceDependency.newBuilder().build();
    }
}
