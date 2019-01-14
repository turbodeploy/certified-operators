package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.StoragePriceBundle;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

public class CostDTOCreator {
    private static final Logger logger = LogManager.getLogger();
    private final MarketPriceTable marketPriceTable;
    private final CommodityConverter commodityConverter;

    public CostDTOCreator(CommodityConverter commodityConverter, MarketPriceTable marketPriceTable) {
        this.commodityConverter = commodityConverter;
        this.marketPriceTable = marketPriceTable;
    }

    // A mapping between the CloudCostDTO.OSType to the os type(string) indicated by the license access commodity
    public static ImmutableMap<CloudCostDTO.OSType, String> OSTypeMapping = ImmutableMap.<CloudCostDTO.OSType, String>builder()
                                        .put(CloudCostDTO.OSType.LINUX, "Linux")
                                        .put(CloudCostDTO.OSType.RHEL, "RHEL")
                                        .put(CloudCostDTO.OSType.SUSE, "SUSE")
                                        .put(CloudCostDTO.OSType.UNKNOWN_OS, "UNKNOWN")
                                        .put(CloudCostDTO.OSType.WINDOWS, "Windows")
                                        .put(CloudCostDTO.OSType.WINDOWS_WITH_SQL_STANDARD, "Windows_SQL_Standard")
                                        .put(CloudCostDTO.OSType.WINDOWS_WITH_SQL_WEB, "Windows_SQL_Web")
                                        .put(CloudCostDTO.OSType.WINDOWS_WITH_SQL_ENTERPRISE, "Windows_SQL_Server_Enterprise")
                                        .put(CloudCostDTO.OSType.WINDOWS_BYOL, "Windows_Bring_your_own_license").build();

    public static ImmutableMap<DatabaseEngine, String> dbEngineMap = ImmutableMap.<DatabaseEngine, String>builder()
                                        .put(DatabaseEngine.AURORA, "Aurora")
                                        .put(DatabaseEngine.MARIADB, "Mariadb")
                                        .put(DatabaseEngine.MYSQL, "MySql")
                                        .put(DatabaseEngine.ORACLE, "Oracle")
                                        .put(DatabaseEngine.POSTGRESQL, "PostgreSql")
                                        .put(DatabaseEngine.SQL_SERVER, "SqlServer")
                                        .put(DatabaseEngine.UNKNOWN, "Unknown").build();

    public static ImmutableMap<DatabaseEdition, String> dbEditionMap = ImmutableMap.<DatabaseEdition, String>builder()
                                        .put(DatabaseEdition.ORACLE_ENTERPRISE, "Enterprise")
                                        .put(DatabaseEdition.ORACLE_STANDARD, "Standard")
                                        .put(DatabaseEdition.ORACLE_STANDARD_1, "Standard One")
                                        .put(DatabaseEdition.ORACLE_STANDARD_2, "Standard Two")
                                        .put(DatabaseEdition.SQL_SERVER_ENTERPRISE, "Enterprise")
                                        .put(DatabaseEdition.SQL_SERVER_STANDARD, "Standard")
                                        .put(DatabaseEdition.SQL_SERVER_WEB, "Web")
                                        .put(DatabaseEdition.SQL_SERVER_EXPRESS, "Express").build();

    public static ImmutableMap<DeploymentType, String> deploymentTypeMap = ImmutableMap.<DeploymentType, String>builder()
                                        .put(DeploymentType.MULTI_AZ, "MultiAz")
                                        .put(DeploymentType.SINGLE_AZ, "SingleAz").build();

    public static ImmutableMap<LicenseModel, String> licenseModelMap = ImmutableMap.<LicenseModel, String>builder()
                                        .put(LicenseModel.BRING_YOUR_OWN_LICENSE, "BringYourOwnLicense")
                                        .put(LicenseModel.LICENSE_INCLUDED, "LicenseIncluded")
                                        .put(LicenseModel.NO_LICENSE_REQUIRED, "NoLicenseRequired").build();

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
        if (priceBundle == null) {
            logger.warn("Failed to get pricing information for tier {} on region {}",
                    tier.getDisplayName(), region.getDisplayName());
            return CostDTO.newBuilder().setComputeTierCost(computeTierDTOBuilder.build()).build();
        }
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (ComputePrice price : priceBundle.getPrices()) {
            List<CommodityType> licenseCommType = licenseCommoditySet.stream()
                    .filter(c -> c.getKey().equals(OSTypeMapping.get(price.getOsType()))).collect(Collectors.toList());
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
        Map<CommodityType, DatabasePrice> priceMapping = new HashMap<>();
        for (DatabasePrice price : priceBundle.getPrices()) {
            if (!baOidSet.contains(price.getAccountId())) {
                logger.warn("Entity in tier {} region {} does not have business account oid {},"
                        + " yet the account is found in cost component",
                        tier.getDisplayName(), region.getDisplayName(), price.getAccountId());
                continue;
            }
            StringBuilder licenseKey = new StringBuilder();
            licenseKey.append(dbEngineMap.get(price.getDbEngine()) != null ? dbEngineMap.get(price.getDbEngine()) : "null")
                    .append(":")
                    .append(dbEditionMap.get(price.getDbEdition()) != null ? dbEditionMap.get(price.getDbEdition()) : "null");
            licenseCommoditySet.stream()
                    .filter(commType -> commType.getKey().contains(licenseKey.toString()))
                    .forEach(commType -> priceMapping.put(commType, price));
        }

        for (long oid : baOidSet) {
            priceMapping.forEach((licenseKey, price)->{
                dbTierDTOBuilder.addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(oid)
                                .setLicenseCommodityType(commodityConverter
                                        .toMarketCommodityId(licenseKey))
                                .setPrice(price.getHourlyPrice())
                                .build());
            });
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
    public CostDTO createStorageTierCostDTO(TopologyEntityDTO tier, TopologyEntityDTO region,
                                            Set<TopologyEntityDTO> businessAccountDTOs) {
        StoragePriceBundle storageCostBundle = marketPriceTable
                        .getStoragePriceBundle(tier.getOid(), region.getOid());
        CostDTO.StorageTierCostDTO.Builder storageDTO =  StorageTierCostDTO.newBuilder();
        if (storageCostBundle == null) {
            logger.warn("Failed to get pricing information for tier {} on region {}",
                    tier.getDisplayName(), region.getDisplayName());
            return CostDTO.newBuilder().setStorageTierCost(storageDTO.build()).build();
        }
        tier.getCommoditySoldListList().forEach(c ->  {
            CommodityType commType = c.getCommodityType();
            // populates the min and max capacity for a given commodity
            if (c.hasMaxAmountForConsumer() && c.hasMinAmountForConsumer()) {
                storageDTO.addStorageResourceLimitation(StorageTierCostDTO
                        .StorageResourceLimitation.newBuilder()
                        .setResourceType(commodityConverter.commoditySpecification(commType))
                        .setMaxCapacity(c.getMaxAmountForConsumer())
                        .setMinCapacity(c.getMinAmountForConsumer())
                        .build());
            }
            // populates the ratio dependency between max this commodity and its base commodity
            if (c.hasRatioDependency()) {
                storageDTO.addStorageResourceDependency(StorageTierCostDTO
                        .StorageResourceDependency.newBuilder()
                        .setBaseResourceType(commodityConverter
                                .commoditySpecification(c.getRatioDependency().getBaseCommodity()))
                        .setDependentResourceType(commodityConverter.commoditySpecification(commType))
                        .setRatio(c.getRatioDependency().getRatio())
                        .build());
            }
            if (!storageCostBundle.getPrices(commType).isEmpty()) {
                StorageTierCostDTO.StorageResourceCost.Builder builder = StorageTierCostDTO.StorageResourceCost
                                .newBuilder()
                                .setResourceType(commodityConverter.commoditySpecification(commType));
                storageCostBundle.getPrices(commType).forEach(p -> builder.addStorageTierPriceData(p));
                storageDTO.addStorageResourceCost(builder.build());
            }
        });

        return CostDTO.newBuilder().setStorageTierCost(storageDTO.build()).build();
    }


    /**
     * Create {@link ComputeResourceDependency} which specifies the constraint between comm bought used
     * and comm sold capacity.
     *
     * @param tier the compute tier topology entity DTO
     * @return ComputeResourceDependency
     */
    public ComputeResourceDependency createComputeResourceDependency(TopologyEntityDTO tier) {
        ComputeResourceDependency.Builder dependency = ComputeResourceDependency.newBuilder();
        // if the compute tier has dedicated storage network state as configured disabled or not supported,
        // the sum of netTpUsed and ioTpUsed should be within the netTpSold capacity so we populate a
        // ComputeResourceDependencyDTO to represent the netTpUsed and ioTpUsed constraint
        if (tier.getTypeSpecificInfo().hasComputeTier()
                && (tier.getTypeSpecificInfo().getComputeTier().getDedicatedStorageNetworkState()
                == CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState.CONFIGURED_DISABLED
                || tier.getTypeSpecificInfo().getComputeTier().getDedicatedStorageNetworkState()
                == CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState.NOT_SUPPORTED)) {
            List<CommoditySoldDTO> netThruPut = tier.getCommoditySoldListList().stream()
                    .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE)
                    .collect(Collectors.toList());
            List<CommoditySoldDTO> ioThruPut = tier.getCommoditySoldListList().stream()
                            .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE)
                            .collect(Collectors.toList());
            if (netThruPut.size() == 1 && ioThruPut.size() == 1) {
                dependency.setBaseResourceType(commodityConverter.commoditySpecification(netThruPut.get(0).getCommodityType()))
                        .setDependentResourceType(commodityConverter.commoditySpecification(ioThruPut.get(0).getCommodityType()))
                        .build();
            }
        }
        return dependency.build();
    }
}
