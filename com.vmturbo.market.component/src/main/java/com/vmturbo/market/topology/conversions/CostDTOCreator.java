package com.vmturbo.market.topology.conversions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.StoragePriceBundle;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

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
        ComputePriceBundle priceBundle = marketPriceTable.getComputePriceBundle(tier, region.getOid());
        if (priceBundle == null) {
            logger.warn("Failed to get pricing information for tier {} on region {}",
                    tier.getDisplayName(), region.getDisplayName());
            return CostDTO.newBuilder().setComputeTierCost(computeTierDTOBuilder.build()).build();
        }
        Map<Long, Map<OSType, ComputePrice>> computePrices = Maps.newHashMap();
        priceBundle.getPrices().forEach(price ->
                computePrices.computeIfAbsent(price.getAccountId(), ba -> Maps.newHashMap())
                        .computeIfAbsent(price.getOsType(), os -> price));
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (long baOid : baOidSet) {
            Map<OSType, ComputePrice> pricesForBa = computePrices.get(baOid);
            if (pricesForBa == null) {
                logger.warn("Cost not found for tier {} - region {} - BA {}",
                        tier.getDisplayName(), region.getDisplayName(), baOid);
            }
            for (CommodityType licenseCommodity : licenseCommoditySet) {
                double price = Double.POSITIVE_INFINITY;
                if (pricesForBa != null) {
                    OSType osType = MarketPriceTable.OS_TYPE_MAP.get(licenseCommodity.getKey());
                    if (osType != null) {
                        ComputePrice computePrice = pricesForBa.get(osType);
                        if (computePrice != null) {
                            price = computePrice.getHourlyPrice();
                            // If the price is a base price, then add this as a cost tuple with
                            // license commodity id -1. Base price is usually the cost of the tier
                            // with LINUX OS. Inside the analysis library, we use this price if the
                            // license the VM is looking for is not found in the costMap. (Although
                            // this should not happen, it is more of a safety mechanism)
                            if (computePrice.isBasePrice()) {
                                computeTierDTOBuilder
                                        .addCostTupleList(createCostTuple(baOid, -1, price));
                            }
                        } else {
                            logger.warn("Cost not found for tier {} - region {} - BA {} - OS {}",
                                    tier.getDisplayName(), region.getDisplayName(), baOid, osType);
                        }
                    } else {
                        logger.warn("Tier {} - region {} sells license {}, but market has no mapping for this license."
                                , tier.getDisplayName(), region.getDisplayName(), licenseCommodity.getKey());
                    }
                }
                computeTierDTOBuilder
                        .addCostTupleList(createCostTuple(baOid, commodityConverter
                                .toMarketCommodityId(licenseCommodity), price));
            }
        }
        Optional<ComputeResourceDependency> dependency = createComputeResourceDependency(tier);
        dependency.map(computeResourceDependency -> computeTierDTOBuilder.addComputeResourceDepedency(computeResourceDependency));

        return CostDTO.newBuilder()
                .setComputeTierCost(computeTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .build())
                .build();
    }

    private CostTuple.Builder createCostTuple(Long baOid, int licenseCommodityId, double hourlyPrice) {
        return CostTuple.newBuilder().setBusinessAccountId(baOid)
                .setLicenseCommodityType(licenseCommodityId)
                .setPrice(hourlyPrice);
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

        Map<Long, Map<String, DatabasePrice>> databasePriceMap = Maps.newHashMap();
        priceBundle.getPrices().forEach(price ->
                databasePriceMap.computeIfAbsent(price.getAccountId(), ba -> Maps.newHashMap())
                        .computeIfAbsent(price.toString(), id -> price));


        for (long baOid : baOidSet) {
            Map<String, DatabasePrice> pricesForBa = databasePriceMap.get(baOid);
            if (pricesForBa == null) {
                logger.warn("Cost not found for tier {} - region {} - BA {}",
                        tier.getDisplayName(), region.getDisplayName(), baOid);
                continue;
            }
            for (CommodityType licenseCommodity : licenseCommoditySet) {
                double price = Double.POSITIVE_INFINITY;
                String licenseId = licenseCommodity.getKey();
                // we support just LicenseIncluded and NoLicenseRequired
                if (licenseId.contains(MarketPriceTable.LICENSE_MODEL_MAP.get(LicenseModel.LICENSE_INCLUDED)) ||
                        licenseId.contains(MarketPriceTable.LICENSE_MODEL_MAP.get(LicenseModel.NO_LICENSE_REQUIRED))) {
                    licenseId = licenseId.replace(MarketPriceTable.LICENSE_MODEL_MAP.get(LicenseModel.LICENSE_INCLUDED), "");
                    licenseId = licenseId.replace(MarketPriceTable.LICENSE_MODEL_MAP.get(LicenseModel.NO_LICENSE_REQUIRED), "");
                    // lookup for license without LicenseModel in the priceMap
                    DatabasePrice databasePrice = pricesForBa.get(licenseId);
                    if (databasePrice != null) {
                        price = databasePrice.getHourlyPrice();
                    } else {
                        // MULTL-AZ licenses are going to be given INFINITE cost
                        logger.trace("Cost not found for tier {} - region {} - BA {} - license {}",
                                tier.getDisplayName(), region.getDisplayName(), baOid, licenseId);
                    }
                } else {
                    // License is for BYOL, return INFINITE price
                    if (logger.isTraceEnabled()) {
                        logger.trace("Returning INFINITE price for {} license sold by tier {} - region {} - BA {}",
                                licenseId, tier.getDisplayName(), region.getDisplayName(), baOid);
                    }
                }
                dbTierDTOBuilder
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(baOid)
                                .setLicenseCommodityType(commodityConverter
                                    .toMarketCommodityId(licenseCommodity))
                                .setPrice(price));
            }
            // price when license isn't available
            dbTierDTOBuilder
                    .addCostTupleList(CostTuple.newBuilder()
                                    .setBusinessAccountId(baOid)
                                    .setLicenseCommodityType(-1)
                                    .setPrice(Double.POSITIVE_INFINITY));

        }

        return CostDTO.newBuilder()
                .setComputeTierCost(dbTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .build())
                .build();
    }

    /**
     * Create the CBTP cost dto
     *
     * @return the CBTP cost DTO
     */
    public CostDTO createCbtpCostDTO() {
        return CostDTO.newBuilder().setCbtpResourceBundle(
                CbtpCostDTO.newBuilder().setCouponBaseType(
                        com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.COUPON_VALUE)
                        .setDiscountPercentage(1)
                        .build()).build();
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
    public Optional<ComputeResourceDependency> createComputeResourceDependency(
            TopologyEntityDTO tier) {
        // if the compute tier has dedicated storage network state as configured disabled or not supported,
        // the sum of netTpUsed and ioTpUsed should be within the netTpSold capacity so we populate a
        // ComputeResourceDependencyDTO to represent the netTpUsed and ioTpUsed constraint
        String tierDisplayName = tier.getDisplayName();
        if (tier.hasTypeSpecificInfo() && tier.getTypeSpecificInfo().hasComputeTier()) {
            ComputeTierInfo computeTierInfo = tier.getTypeSpecificInfo().getComputeTier();
            if (computeTierInfo.hasDedicatedStorageNetworkState()) {
                ComputeTierData.DedicatedStorageNetworkState tierNetworkState =
                        computeTierInfo.getDedicatedStorageNetworkState();
                if (CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState.CONFIGURED_DISABLED ==
                        tierNetworkState ||
                        CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState.NOT_SUPPORTED ==
                                tierNetworkState) {

                    Map<Integer, List<CommoditySoldDTO>> typeToCommodities =
                            tier.getCommoditySoldListList()
                                    .stream()
                                    .collect(Collectors.groupingBy(
                                            c -> c.getCommodityType().getType()));

                    List<CommoditySoldDTO> netThruPut =
                            typeToCommodities.get(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE);

                    List<CommoditySoldDTO> ioThruPut =
                            typeToCommodities.get(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE);

                    if (netThruPut.size() == 1 && ioThruPut.size() == 1) {
                        ComputeResourceDependency.Builder dependency =
                                ComputeResourceDependency.newBuilder();
                        dependency.setBaseResourceType(commodityConverter.commoditySpecification(
                                netThruPut.get(0).getCommodityType()))
                                .setDependentResourceType(commodityConverter.commoditySpecification(
                                        ioThruPut.get(0).getCommodityType()));
                        return Optional.of(dependency.build());
                    } else {
                        logger.warn("Expected one commodity each of IOThroughput" +
                                " and NetThroughput, Found zero/multiple for {}", tierDisplayName);
                    }
                } else {
                    logger.trace("No dependency added since" +
                            " dedicated storage network is enabled for {}. ", tierDisplayName);
                }
            } else {
                logger.debug("No dependency added since" +
                        " dedicated storage network information not available {}", tierDisplayName);
            }
        } else {
            logger.debug("No compute tier associated with {} ", tierDisplayName);
        }
        return Optional.empty();
    }

}
