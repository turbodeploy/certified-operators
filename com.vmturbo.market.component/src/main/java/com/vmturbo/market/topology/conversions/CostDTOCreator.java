package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
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
import com.vmturbo.market.runner.cost.MarketPriceTable.CoreBasedLicensePriceBundle;
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
     * @param uniqueAccountPricingData The unique set of pricing in the topology.
     *
     * @return CostDTO
     */
    public CostDTO createCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
            Set<AccountPricingData> uniqueAccountPricingData) {
        if (tier.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            return createComputeTierCostDTO(tier, regions, uniqueAccountPricingData);
        } else {
            return createDatabaseTierCostDTO(tier, regions, uniqueAccountPricingData);
        }
    }

    /**
     * Create CostDTO for a given tier and region based traderDTO.
     *
     * @param tier the compute tier topology entity DTO.
     * @param regions list of region topology entity DTOs.
     * @param uniqueAccountPricingData The set of unique account pricing data.
     *
     * @return CostDTO
     */
    public CostDTO createComputeTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
                                            Set<AccountPricingData> uniqueAccountPricingData) {
        ComputeTierCostDTO.Builder computeTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                ComputePriceBundle priceBundle = marketPriceTable.getComputePriceBundle(tier, region.getOid(), accountPricingData);
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
                    logger.warn("Cost not found for tier {} - region {} - AccountPricingDataOid {}",
                            tier.getDisplayName(), region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
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
                        CommoditySpecificationTO spec = commodityConverter
                                .commoditySpecification(licenseCommodity);
                        computeTierDTOBuilder
                                .addCostTupleList(CostTuple.newBuilder()
                                        .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                                        .setRegionId(region.getOid())
                                        .setLicenseCommodityType(spec.getType())
                                        .setPrice(price));
                    }
                }
            }
        }
        createComputeResourceDependency(tier)
                .map(computeTierDTOBuilder::addComputeResourceDepedency);

        return CostDTO.newBuilder()
                .setComputeTierCost(computeTierDTOBuilder
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .setRegionCommodityBaseType(CommodityDTO.CommodityType.DATACENTER_VALUE)
                        .build())
                .build();
    }

    private CostTuple.Builder createCostTuple(long baOid, int licenseCommodityId, double hourlyPrice,
                                              long regionId) {
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
     * @param uniqueAccountPricingData The set of unique account pricing data.
     *
     * @return CostDTO
     */
    private CostDTO createDatabaseTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
                                             Set<AccountPricingData> uniqueAccountPricingData) {
        ComputeTierCostDTO.Builder dbTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                DatabasePriceBundle priceBundle = marketPriceTable.getDatabasePriceBundle(tier.getOid(),
                        region.getOid(), accountPricingData);
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
                    logger.warn("Cost not found for tier {} - region {} - AccountPricingDataOid {}",
                            tier.getDisplayName(), region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
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
                            logger.trace("Cost not found for tier {} - region {} - AccountPricingDataOid {} - license {}",
                                tier.getDisplayName(), region.getDisplayName(),
                                accountPricingData.getAccountPricingDataOid(), licenseId);
                        }
                    } else {
                        // License is for BYOL, return INFINITE price
                        if (logger.isTraceEnabled()) {
                            logger.trace("Returning INFINITE price for {} license sold by tier {} - region {}",
                                    licenseId, tier.getDisplayName(), region.getDisplayName());
                        }
                    }
                    CommoditySpecificationTO spec = commodityConverter
                            .commoditySpecification(licenseCommodity);
                    dbTierDTOBuilder
                            .addCostTupleList(createCostTuple(accountPricingData.getAccountPricingDataOid(),
                                    spec.getType(), price, region.getOid()));
                }
                // price when license isn't available
                CommoditySpecificationTO spec = commodityConverter
                        .commoditySpecification(dataCenterAccessCommodity.get());
                dbTierDTOBuilder
                        .addCostTupleList(CostTuple.newBuilder()
                                .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                                .setLicenseCommodityType(-1)
                                .setRegionId(dataCenterAccessCommodity.isPresent() ?
                                        spec.getType() : null)
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
     * @param computeTiersInScope The compute tier in scope of the RI.
     * @param applicableBusinessAccounts account scope of the cbtp.
     *
     * @return The CBTP cost dto.
     */
    CostDTO createCbtpCostDTO(final ReservedInstanceKey reservedInstanceKey,
                              Map<Long, AccountPricingData> accountPricingDataByBusinessAccountOid,
                              TopologyEntityDTO region,
                              Set<TopologyEntityDTO> computeTiersInScope,
                              final Set<Long> applicableBusinessAccounts) {
        Set<CostTuple> costTupleList = new HashSet<>();
        for (final AccountPricingData accountPricingData: accountPricingDataByBusinessAccountOid.values()) {

            if (accountPricingData != null) {
                Set<CostTuple> cbtpCostTuples = createCbtpCostTuples(
                        reservedInstanceKey,
                        accountPricingData,
                        region,
                        computeTiersInScope);

                costTupleList.addAll(cbtpCostTuples);
            }
        }

        // Set CBTP scope to billing family (for shared RI) or account (for single scoped RI)
        final long scopeId;
        if (reservedInstanceKey.getShared()) {
            scopeId = reservedInstanceKey.getAccountScopeId();
        } else if (!applicableBusinessAccounts.isEmpty()) {
            // For single scoped RI applicable accounts set must contain a single element
            if (applicableBusinessAccounts.size() > 1) {
                logger.error("More than one applicable account for " + reservedInstanceKey);
            }
            scopeId = applicableBusinessAccounts.iterator().next();
        } else {
            logger.error("Empty applicable accounts list for " + reservedInstanceKey);
            scopeId = reservedInstanceKey.getAccountScopeId();
        }

        return CostDTO.newBuilder()
                .setCbtpResourceBundle(CbtpCostDTO.newBuilder()
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .addAllCostTupleList(costTupleList)
                        .setDiscountPercentage(1)
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setScopeId(scopeId))
                .build();
    }

    /**
     * Creates a cost tuple, scoped to the unique price id of the {@code accountPricingData}. The price ID
     * of the cost tuple may differ from the scope IDs supported by the CBTP. However, the CBTP is still
     * correctly scoped through the scope IDs and only after passing the scope filter is pricing applied.
     * This limits the CBTP to the correct scope, while benefiting from the consolidation of pricing
     * similar to template providers.
     *
     * @param riKey The {@link ReservedInstanceKey} of the representative RI of the CBTP
     * @param accountPricingData The {@link AccountPricingData} supported by the CBTP. The CBTP may support
     *                           multiple unique price IDs.
     * @param region The region of the CBTP
     * @param computeTiersInScope The compute tiers in scope of the CBTP. The supported compute tiers are
     *                            used to determine the reserved license rates of the CBTP.
     * @return The set of cost tuples (one per supported OS) for the {@code accountPricingData}
     */
    @Nonnull
    private Set<CostDTOs.CostDTO.CostTuple> createCbtpCostTuples(final ReservedInstanceKey riKey,
                                                                 final AccountPricingData accountPricingData,
                                                                 final TopologyEntityDTO region,
                                                                 final Set<TopologyEntityDTO> computeTiersInScope) {

        Map<OSType, CostTuple.Builder> costTupleBuilderByOs = new HashMap<>();
        Map<OSType, Double> maxComputePriceByOS = new HashMap<>();

        // For each compute tier in scope of the RI, we need to send the reserved license rates
        // as part of the cost tuple. We also iterator over the compute tiers, selecting the tier with the
        // largest cost as the basis for the nominal fee of the CBPT (if there are no associated reserved
        // license rates to differentiate the RIs)
        for (TopologyEntityDTO computeTier : computeTiersInScope) {

            final ComputePriceBundle computePriceBundle = marketPriceTable.getComputePriceBundle(computeTier,
                    region.getOid(), accountPricingData);

            computePriceBundle.getPrices().forEach(computePrice -> {
                        if (computePrice.getHourlyPrice() >
                                maxComputePriceByOS.getOrDefault(computePrice.getOsType(), 0.0)) {

                            maxComputePriceByOS.put(computePrice.getOsType(), computePrice.getHourlyPrice());
                        }
                    });

            final Set<CoreBasedLicensePriceBundle> reservedLicensePriceBundles =
                    marketPriceTable.getReservedLicensePriceBundles(accountPricingData, computeTier)
                            .stream()
                            // If the RI supports a smaller set of OS types than the compute tiers in scope
                            // of the RI, we need to filter the license price bundles to only those supported
                            // by the RI.
                            .filter(licenseBundle -> riKey.isPlatformFlexible() ||
                                    licenseBundle.osType() == riKey.getOs())
                            .collect(ImmutableSet.toImmutableSet());

            for (CoreBasedLicensePriceBundle licenseBundle : reservedLicensePriceBundles) {

                final CostTuple.Builder costTupleBuilder =
                        costTupleBuilderByOs.computeIfAbsent(licenseBundle.osType(), osType ->
                                CostTuple.newBuilder()
                                        .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                                        .setLicenseCommodityType(
                                            commodityConverter.commoditySpecification(
                                                    licenseBundle.licenseCommodityType()).getType()));

                // Is it assumed all compute tiers within scope of an RI will
                // sell the same VCORE commodity
                licenseBundle.vcoreCommodityType().ifPresent(commType ->
                        costTupleBuilder.setCoreCommodityType(
                                commodityConverter.commoditySpecification(commType).getType()));

                licenseBundle.price().ifPresent(price ->
                        costTupleBuilder.putPriceByNumCores(licenseBundle.numCores(), price));
            }
        }

        costTupleBuilderByOs.forEach((osType, costTupleBuilder) -> {

            // Set the zone and region scopes
            setZoneIdAndRegionInfo(riKey, costTupleBuilder);

            // If all license prices for an OS are 0, set the base price to a fractional value based
            // on the largest compute tier. Note that the nominal fee is not used as a differentiator
            // when a license fee is present. This is an intentional decision to not mix actual costs
            // (the reserved license rates) with artifical costs (the nominal fees). It is unclear how
            // to mix actual and artificial costs, when the nominal fee is based on the largest instance
            // size supported by a CBTP and the number of sizes within each family are not uniform.
            boolean addNominalFee = costTupleBuilder.getPriceByNumCoresMap().values()
                    .stream()
                    .allMatch(p -> p <= 0.0);
            if (addNominalFee) {
                costTupleBuilder.setPrice(maxComputePriceByOS.getOrDefault(osType, 0.0) * riCostDeprecationFactor);
            }
        });

        return costTupleBuilderByOs.values().stream()
                .map(CostTuple.Builder::build)
                .collect(ImmutableSet.toImmutableSet());
    }


    /**
     * Sets the ri zone and region info on the cbtp cost dto builder.
     *
     * @param riKey Info about the reserved instance key object.
     * @param builder The cbtp cost dto builder.
     */
    private void setZoneIdAndRegionInfo(ReservedInstanceKey riKey, CostTuple.Builder builder) {
        if (riKey.getZoneId() != 0) {
            builder.setZoneId(riKey.getZoneId());
        } else {
            builder.setRegionId(riKey.getRegionId());
        }
    }

    /**
     * Create CostDTO for a given storage tier and region based traderDTO.
     *
     * @param tier the storage tier topology entity DTO.
     * @param connectedRegions the region topology entity DTO.
     * @param uniqueAccountPricingData The unique account pricing data in the system.
     *
     * @return CostDTO
     */
    public CostDTO createStorageTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> connectedRegions,
                                            Set<AccountPricingData> uniqueAccountPricingData) {
        CostDTO.StorageTierCostDTO.Builder storageDTO =  StorageTierCostDTO.newBuilder();
        StorageTierCostDTO.StorageResourceCost.Builder builder = StorageTierCostDTO.StorageResourceCost
                .newBuilder();
        for (TopologyEntityDTO region: connectedRegions) {
            for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
                StoragePriceBundle storageCostBundle = marketPriceTable
                        .getStoragePriceBundle(tier.getOid(), region.getOid(), accountPricingData);
                if (storageCostBundle == null) {
                    // This can happen in unit tests
                    logger.warn("Failed to get pricing information for tier {} on region {}",
                            tier.getDisplayName(), region.getDisplayName());
                    return CostDTO.newBuilder().setStorageTierCost(storageDTO.build()).build();
                }
                tier.getCommoditySoldListList().forEach(c -> {
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
                        builder.setResourceType(commodityConverter.commoditySpecification(commType));
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
    private Optional<ComputeResourceDependency> createComputeResourceDependency(
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
