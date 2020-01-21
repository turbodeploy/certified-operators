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
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.StoragePriceBundle;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
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
    private static final double riCostDeprecationFactor = 0.00001;

    public CostDTOCreator(CommodityConverter commodityConverter, MarketPriceTable marketPriceTable) {
        this.commodityConverter = commodityConverter;
        this.marketPriceTable = marketPriceTable;
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier is topology entity DTO representation of the tier.
     * @param regions list of region topology entity DTOs.
     * @param businessAccountDTOs all business accounts in the topology.
     * @param uniqueAccountPricingData The unique set of pricing in the topology.
     *
     * @return CostDTO
     */
    public CostDTO createCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions, Set<TopologyEntityDTO> businessAccountDTOs, Set<AccountPricingData> uniqueAccountPricingData) {
        if (tier.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            return createComputeTierCostDTO(tier, regions, businessAccountDTOs, uniqueAccountPricingData );
        } else {
            return createDatabaseTierCostDTO(tier, regions, businessAccountDTOs, uniqueAccountPricingData);
        }
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier the compute tier topology entity DTO.
     * @param regions list of region topology entity DTOs.
     * @param businessAccountDTOs all business accounts in the topology.
     * @param uniqueAccountPricingData The set of unique account pricing data.
     *
     * @return CostDTO
     */
    public CostDTO createComputeTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions, Set<TopologyEntityDTO> businessAccountDTOs,
                                            Set<AccountPricingData> uniqueAccountPricingData) {
        ComputeTierCostDTO.Builder computeTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                ComputePriceBundle priceBundle = marketPriceTable.getComputePriceBundle(tier, region.getOid(), accountPricingData);
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

                Map<OSType, ComputePrice> pricesForBa = computePrices.get(accountPricingData.getAccountPricingDataOid());
                if (pricesForBa == null) {
                    logger.warn("Cost not found for tier {} - region {} - BA {}",
                            tier.getDisplayName(), region.getDisplayName(), accountPricingData);
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
                                            .addCostTupleList(createCostTuple(accountPricingData.getAccountPricingDataOid(), -1,
                                                    price, region.getOid()));
                                }
                            } else {
                                logger.warn("Tier {} - region {} sells license {}, but market has no" +
                                        " mapping for this license.", tier.getDisplayName(),
                                        region.getDisplayName(), licenseCommodity.getKey());
                            }
                        }
                        computeTierDTOBuilder
                                .addCostTupleList(CostTuple.newBuilder()
                                        .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                                        .setRegionId(region.getOid())
                                        .setLicenseCommodityType(commodityConverter
                                                .toMarketCommodityId(licenseCommodity))
                                        .setPrice(price));
                    }
                }
            }
        }
        Optional<ComputeResourceDependency> dependency = createComputeResourceDependency(tier);
        dependency.map(computeResourceDependency -> computeTierDTOBuilder.addComputeResourceDepedency(computeResourceDependency));

        return CostDTO.newBuilder()
                .setComputeTierCost(computeTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .setRegionCommodityBaseType(CommodityDTO.CommodityType.DATACENTER_VALUE)
                        .build())
                .build();
    }

    private CostTuple.Builder  createCostTuple(Long baOid, Integer licenseCommodityId, double hourlyPrice,
                                              Long regionId) {
        return CostTuple.newBuilder().setBusinessAccountId(baOid)
                .setLicenseCommodityType(licenseCommodityId)
                .setRegionId(regionId)
                .setPrice(hourlyPrice);
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier the database tier topology entity DTO
     * @param regions the regions topology entity DTO
     * @param businessAccountDTOs all business accounts in the topology
     * @param uniqueAccountPricingData The set of unique account pricing data.
     *
     * @return CostDTO
     */
    public CostDTO createDatabaseTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
                                             Set<TopologyEntityDTO> businessAccountDTOs,
                                             Set<AccountPricingData> uniqueAccountPricingData) {
        ComputeTierCostDTO.Builder dbTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        Set<Long> baOidSet = businessAccountDTOs.stream().map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                DatabasePriceBundle priceBundle = marketPriceTable.getDatabasePriceBundle(tier.getOid(),
                        region.getOid(), accountPricingData);
                if (priceBundle == null) {
                    logger.warn("Failed to get pricing information for tier {} on region {}",
                            tier.getDisplayName(), region.getDisplayName());
                    return CostDTO.newBuilder().setComputeTierCost(dbTierDTOBuilder.build()).build();
                }
                Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                        .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .map(CommoditySoldDTO::getCommodityType)
                        .collect(Collectors.toSet());

                Map<Long, Map<String, DatabasePrice>> databasePriceMap = Maps.newHashMap();
                priceBundle.getPrices().forEach(price ->
                        databasePriceMap.computeIfAbsent(price.getAccountId(), ba -> Maps.newHashMap())
                                .computeIfAbsent(price.toString(), id -> price));
                Optional<CommodityType> dataCenterAccessCommodity = Optional.ofNullable(region.getCommoditySoldListList()
                        .get(0).getCommodityType());

                Map<String, DatabasePrice> pricesForBa = databasePriceMap.get(accountPricingData.getAccountPricingDataOid());
                if (pricesForBa == null) {
                    logger.warn("Cost not found for tier {} - region {} - BA {}",
                            tier.getDisplayName(), region.getDisplayName());
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
                            logger.trace("Cost not found for tier {} - region {} - BA - license {}",
                                    tier.getDisplayName(), region.getDisplayName(), licenseId);
                        }
                    } else {
                        // License is for BYOL, return INFINITE price
                        if (logger.isTraceEnabled()) {
                            logger.trace("Returning INFINITE price for {} license sold by tier {} - region {} - BA {}",
                                    licenseId, tier.getDisplayName(), region.getDisplayName());
                        }
                    }
                    dbTierDTOBuilder
                            .addCostTupleList(createCostTuple(accountPricingData.getAccountPricingDataOid(), commodityConverter
                                    .toMarketCommodityId(licenseCommodity), price, region.getOid()));
                }
                // price when license isn't available
                dbTierDTOBuilder
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                                .setLicenseCommodityType(-1)
                                .setRegionId(dataCenterAccessCommodity.isPresent() ?
                                        commodityConverter.toMarketCommodityId(dataCenterAccessCommodity.get()) : null)
                                .setPrice(Double.POSITIVE_INFINITY));

            }
        }

        return CostDTO.newBuilder()
                .setComputeTierCost(dbTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .build())
                .build();
    }

    /**
     * Create the CBTP cost dto.
     *
     * @param reservedInstanceKey reservedInstanceKey of the RI for which the CostDTO is created.
     * @param accountPricingDataByBusinessAccountOid The accountPricing to use for the specific business account.
     * @param region The region the RIDiscountedMarketTier is being created for.
     * @param computeTier The compute tier based on which the price of the RIDiscountedMarketTier is calculated.
     *
     * @return The CBTP cost dto.
     */
    CostDTO createCbtpCostDTO(final ReservedInstanceKey reservedInstanceKey, Map<Long, AccountPricingData> accountPricingDataByBusinessAccountOid,
                              TopologyEntityDTO region, TopologyEntityDTO computeTier) {
        Optional<ComputePrice> templatePrice = Optional.empty();
        AccountPricingData accountPricingData = accountPricingDataByBusinessAccountOid.get(reservedInstanceKey.getAccount());
        if (accountPricingData != null) {
            ComputePriceBundle priceBundle = marketPriceTable.getComputePriceBundle(computeTier, region.getOid(), accountPricingData);
            templatePrice = priceBundle.getPrices().stream().filter(s ->s.getOsType().equals(reservedInstanceKey.getOs())).findFirst();
        }
        return CostDTO.newBuilder().setCbtpResourceBundle(
                CbtpCostDTO.newBuilder().setCouponBaseType(
                        CommodityDTO.CommodityType
                                .COUPON_VALUE)
                        .setDiscountPercentage(1)
                        .setCostTuple(addLocationAndPriceInfo(CostTuple.newBuilder(), reservedInstanceKey, templatePrice))
                        .build()).build();
    }

    /**
     * Add the location and pricing info to the RI discounted market tier cost dto.
     *
     * @param builder The CostDTO builder
     * @param riKey reservedInstanceKey of the RI for which the CostDTO is created.
     * @param price The pricing of the corresponding on demand compute tier template.
     *
     * @return The Cost DTO builder with the parameters set.
     */
    private CostDTOs.CostDTO.CostTuple.Builder addLocationAndPriceInfo(
            CostDTOs.CostDTO.CostTuple.Builder builder, final ReservedInstanceKey riKey, Optional<ComputePrice> price) {
        if (price.isPresent()) {
            builder.setPrice(price.get().getHourlyPrice() * riCostDeprecationFactor);
        }
        return riKey.getZoneId() != 0 ? builder.setZoneId(riKey.getZoneId())
                : builder.setRegionId(riKey.getRegionId());
    }

    /**
     * Create CostDTO for a given storage tier and region based traderDTO.
     *
     * @param tier the storage tier topology entity DTO.
     * @param connectedRegions the region topology entity DTO.
     * @param businessAccountDTOs all business accounts in the topology.
     * @param uniqueAccountPricingData The unique account pricing data in the system.
     *
     * @return CostDTO
     */
    public CostDTO createStorageTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> connectedRegions,
                                            Set<TopologyEntityDTO> businessAccountDTOs, Set<AccountPricingData> uniqueAccountPricingData) {
        CostDTO.StorageTierCostDTO.Builder storageDTO =  StorageTierCostDTO.newBuilder();
        StorageTierCostDTO.StorageResourceCost.Builder builder = StorageTierCostDTO.StorageResourceCost
                .newBuilder();
        for (TopologyEntityDTO region: connectedRegions) {
            for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
                StoragePriceBundle storageCostBundle = marketPriceTable
                        .getStoragePriceBundle(tier.getOid(), region.getOid(), accountPricingData);
                if (storageCostBundle == null) {
                    logger.warn("Failed to get pricing information for tier {} on region {}",
                            tier.getDisplayName(), region.getDisplayName());
                    return CostDTO.newBuilder().setStorageTierCost(storageDTO.build()).build();
                }
                final int numberOfSlots = tier.getAnalysisSettings().getSlots();
                tier.getCommoditySoldListList().forEach(c -> {
                    CommodityType commType = c.getCommodityType();
                    final List<CommoditySpecificationTO> commSpecs =
                            commodityConverter.commoditySpecification(commType, 1);
                    if (commSpecs.size() > 1) {
                        logger.warn("Multiple commodity specs obtained for {}", numberOfSlots);
                    }
                    // populates the min and max capacity for a given commodity
                    if (c.hasMaxAmountForConsumer() && c.hasMinAmountForConsumer()) {

                        storageDTO.addStorageResourceLimitation(StorageTierCostDTO
                                .StorageResourceLimitation.newBuilder()
                                .setResourceType(commSpecs.get(0))
                                .setMaxCapacity(c.getMaxAmountForConsumer())
                                .setMinCapacity(c.getMinAmountForConsumer())
                                .build());
                    }
                    // populates the ratio dependency between max this commodity and its base commodity
                    if (c.hasRatioDependency()) {
                        final List<CommoditySpecificationTO> baseCommSpecs =
                                commodityConverter.commoditySpecification(c.getRatioDependency().getBaseCommodity(), numberOfSlots);
                        if (baseCommSpecs.size() > 1) {
                            logger.warn("Multiple commodity specs obtained for {}", commType);
                        }
                        storageDTO.addStorageResourceDependency(StorageTierCostDTO
                                .StorageResourceDependency.newBuilder()
                                .setBaseResourceType(baseCommSpecs.get(0))
                                .setDependentResourceType(commSpecs.get(0))
                                .setRatio(c.getRatioDependency().getRatio())
                                .build());
                    }
                    final List<CommoditySpecificationTO> dependentCommSpecs =
                            commodityConverter.commoditySpecification(commType, numberOfSlots);
                    if (dependentCommSpecs.size() > 1) {
                        logger.warn("Multiple commodity specs obtained for {}", commType);
                    }
                    if (!storageCostBundle.getPrices(commType).isEmpty()) {
                        builder.setResourceType(commSpecs.get(0));
                        storageCostBundle.getPrices(commType).forEach(p -> builder.addStorageTierPriceData(p));
                        storageDTO.addStorageResourceCost(builder.build());
                    }
                });
            }
        }
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

                    int slots = tier.getAnalysisSettings().getSlots();
                    if (netThruPut.size() == 1 && ioThruPut.size() == 1) {
                        final CommodityType netType = netThruPut.get(0).getCommodityType();
                        final List<CommoditySpecificationTO> netThruPutSpecs =
                                commodityConverter.commoditySpecification(netType, slots);
                        if (netThruPutSpecs.size() > 1) {
                            logger.warn("Multiple specs obtained for {}", netType);
                        }
                        final CommodityType ioType = ioThruPut.get(0).getCommodityType();
                        final List<CommoditySpecificationTO> ioThruPutSpecs =
                                commodityConverter.commoditySpecification(ioType, slots);
                        if (ioThruPutSpecs.size() > 1) {
                            logger.warn("Multiple specs obtained for {}", ioType);
                        }
                        ComputeResourceDependency.Builder dependency =
                                ComputeResourceDependency.newBuilder();
                        dependency.setBaseResourceType(netThruPutSpecs.get(0))
                                .setDependentResourceType(ioThruPutSpecs.get(0));
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
