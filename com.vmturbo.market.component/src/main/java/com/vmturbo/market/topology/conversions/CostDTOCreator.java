package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.StoragePriceBundle;
import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle;
import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle.DatabasePrice.StorageOption;
import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle;
import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle.DatabaseServerPrice;
import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle.DatabaseServerPrice.DbsStorageOption;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple.DependentResourceOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseServerTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.RangeTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class CostDTOCreator {
    private static final Logger logger = LogManager.getLogger();
    private final CloudRateExtractor marketCloudRateExtractor;
    private final CommodityConverter commodityConverter;
    private static final double riCostDeprecationFactor = 0.00001;

    /**
     * The license map.
     */
    private static final Map<LicenseModel, String> LICENSE_MODEL_MAP = ImmutableMap.<LicenseModel, String>builder()
            .put(LicenseModel.BRING_YOUR_OWN_LICENSE, "BringYourOwnLicense")
            .put(LicenseModel.LICENSE_INCLUDED, "LicenseIncluded")
            .put(LicenseModel.NO_LICENSE_REQUIRED, "NoLicenseRequired").build();

    /**
     * Constructor for the cost dto creator.
     *
     * @param commodityConverter The commodity converter.
     * @param marketCloudRateExtractor The market cloud rate info extractor.
     */
    public CostDTOCreator(CommodityConverter commodityConverter, CloudRateExtractor marketCloudRateExtractor) {
        this.commodityConverter = commodityConverter;
        this.marketCloudRateExtractor = marketCloudRateExtractor;
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
            Set<AccountPricingData<TopologyEntityDTO>> uniqueAccountPricingData) {
        switch (tier.getEntityType()) {
            case EntityType.COMPUTE_TIER_VALUE:
                return createComputeTierCostDTO(tier, regions, uniqueAccountPricingData);
            case EntityType.DATABASE_TIER_VALUE:
                return createDatabaseTierCostDTO(tier, regions, uniqueAccountPricingData);
            case EntityType.DATABASE_SERVER_TIER_VALUE:
                return createDatabaseServerTierCostDTO(tier, regions, uniqueAccountPricingData);
            default:
                logger.error("Cannot find cost creator for: {}", tier.getEntityType());
                return CostDTO.newBuilder().build();
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
                                            Set<AccountPricingData<TopologyEntityDTO>> uniqueAccountPricingData) {
        ComputeTierCostDTO.Builder computeTierDTOBuilder = ComputeTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                CloudRateExtractor.ComputePriceBundle priceBundle = marketCloudRateExtractor.getComputePriceBundle(tier, region.getOid(), accountPricingData);
                Map<Long, Map<OSType, ComputePrice>> computePrices = Maps.newHashMap();
                priceBundle.getPrices().forEach(price ->
                        computePrices.computeIfAbsent(price.accountId(), ba -> Maps.newHashMap())
                                .computeIfAbsent(price.osType(), os -> price));

                Map<OSType, ComputePrice> pricesForBa = computePrices.get(accountPricingData.getAccountPricingDataOid());
                if (pricesForBa == null) {
                    logger.debug("Cost not found for compute tier {} - region {} - AccountPricingDataOid {}",
                            tier.getDisplayName(), region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
                }
                final Set<CommodityType> licenseCommoditySet = getLicenseCommodities(tier);
                for (CommodityType licenseCommodity : licenseCommoditySet) {
                    double price = Double.POSITIVE_INFINITY;
                    if (pricesForBa != null) {
                        OSType osType = CloudRateExtractor.OS_TYPE_MAP.get(licenseCommodity.getKey());
                        if (osType != null) {
                            ComputePrice computePrice = pricesForBa.get(osType);
                            if (computePrice != null) {
                                price = computePrice.hourlyPrice();
                                // If the price is a base price, then add this as a cost tuple with
                                // license commodity id -1. Base price is usually the cost of the tier
                                // with LINUX OS. Inside the analysis library, we use this price if the
                                // license the VM is looking for is not found in the costMap. (Although
                                // this should not happen, it is more of a safety mechanism)
                                if (computePrice.isBasePrice()) {
                                    computeTierDTOBuilder.addCostTupleList(createCostTuple(
                                            accountPricingData.getAccountPricingDataOid(), -1,
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

    private CostTuple.Builder createCostTuple(long baOid, int licenseCommodityId,
            double hourlyPrice, long regionId) {
        return CostTuple.newBuilder()
                .setBusinessAccountId(baOid)
                .setLicenseCommodityType(licenseCommodityId)
                .setRegionId(regionId)
                .setPrice(hourlyPrice);
    }

    private CostTuple.Builder createCostTupleWithDependent(long baOid, int licenseCommodityId,
            double hourlyPrice, long regionId,
            @Nonnull final List<DependentCostTuple> dependentCostTuples) {
        return CostTuple.newBuilder()
                .setBusinessAccountId(baOid)
                .setLicenseCommodityType(licenseCommodityId)
                .setRegionId(regionId)
                .setPrice(hourlyPrice)
                .addAllDependentCostTuples(dependentCostTuples);
    }


    @Nonnull
    private DependentResourceOption convertStorageOptionToResourceOption(
            @Nonnull final StorageOption storageOption) {
        return DependentResourceOption.newBuilder()
                .setAbsoluteIncrement(storageOption.getIncrement())
                .setEndRange(storageOption.getEndRange())
                .setPrice(storageOption.getPrice())
                .build();
    }

    @Nonnull
    private DependentResourceOption convertStorageOptionToResourceOption(
            @Nonnull final DbsStorageOption storageOption) {
        return DependentResourceOption.newBuilder()
                .setAbsoluteIncrement(storageOption.getIncrement())
                .setEndRange(storageOption.getEndRange())
                .setPrice(storageOption.getPrice())
                .build();
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
    @VisibleForTesting
    protected CostDTO createDatabaseTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
                                             Set<AccountPricingData<TopologyEntityDTO>> uniqueAccountPricingData) {
        DatabaseTierCostDTO.Builder dbTierDTOBuilder = DatabaseTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                DatabasePriceBundle priceBundle = marketCloudRateExtractor.getDatabasePriceBundle(tier.getOid(),
                        region.getOid(), accountPricingData);

                Map<Long, Map<String, DatabasePrice>> databasePriceMap = Maps.newHashMap();
                priceBundle.getPrices().forEach(price ->
                        databasePriceMap.computeIfAbsent(price.getAccountId(), ba -> Maps.newHashMap())
                                .computeIfAbsent(price.toString(), id -> price));

                Map<String, DatabasePrice> pricesForBa = databasePriceMap.get(accountPricingData.getAccountPricingDataOid());
                if (pricesForBa == null) {
                    logger.debug("Cost not found for tier {} - region {} - AccountPricingDataOid {}",
                            tier.getDisplayName(), region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
                    continue;
                }
                final Set<CommodityType> licenseCommoditySet = getLicenseCommodities(tier);
                for (CommodityType licenseCommodity : licenseCommoditySet) {
                    double price = Double.POSITIVE_INFINITY;
                    String licenseId = licenseCommodity.getKey();
                    // Keeps the storage options of the Azure DB tiers
                    List<StorageOption> storageOptions = new ArrayList<>();
                    // we support just LicenseIncluded and NoLicenseRequired
                    if (licenseId.contains(LICENSE_MODEL_MAP.get(LicenseModel.LICENSE_INCLUDED))
                            || licenseId.contains(LICENSE_MODEL_MAP.get(LicenseModel.NO_LICENSE_REQUIRED))) {
                        licenseId = normalizeLicenseId(licenseId);
                        // lookup for license without LicenseModel in the priceMap
                        DatabasePrice databasePrice = pricesForBa.get(licenseId);
                        if (databasePrice != null) {
                            price = databasePrice.getHourlyPrice();
                            storageOptions.addAll(databasePrice.getStorageOptions());
                        } else {
                            // MULTL-AZ licenses are going to be given INFINITE cost
                            logger.trace("Cost not found for db tier {} - region {} - AccountPricingDataOid {} - license {}",
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
                    // Add storage options for Azure DB tiers
                    List<DependentCostTuple> dependentCostTuples = new ArrayList<>();
                    if (!storageOptions.isEmpty()) {
                        List<DependentCostTuple.DependentResourceOption> dependentResourceOptions = new ArrayList<>();
                        storageOptions.forEach(storageOption -> {
                            dependentResourceOptions.add(
                                    convertStorageOptionToResourceOption(storageOption));
                        });
                        dependentCostTuples.add(DependentCostTuple.newBuilder()
                                .setDependentResourceType(commodityConverter.commoditySpecification(
                                        CommodityType.newBuilder()
                                                .setType(
                                                        CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                                                .build()).getType())
                                .addAllDependentResourceOptions(dependentResourceOptions)
                                .build());
                        CommoditySpecificationTO spec =
                                commodityConverter.commoditySpecification(licenseCommodity);
                        dbTierDTOBuilder.addCostTupleList(
                                createCostTupleWithDependent(accountPricingData.getAccountPricingDataOid(), spec.getType(), price, region.getOid(),
                                        dependentCostTuples));
                    } else {
                        logger.debug("Storage Options are empty for {}, {}", tier.getDisplayName(), region.getDisplayName());
                    }

                }
                // price when license isn't available
                dbTierDTOBuilder.addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                        .setLicenseCommodityType(-1)
                        .setRegionId(region.getOid())
                        .setPrice(Double.POSITIVE_INFINITY));
            }
        }

        return CostDTO.newBuilder()
                .setDatabaseTierCost(dbTierDTOBuilder.setLicenseCommodityBaseType(
                        CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .build())
                .build();
    }

    protected CostDTO createDatabaseServerTierCostDTO(TopologyEntityDTO tier, List<TopologyEntityDTO> regions,
            Set<AccountPricingData<TopologyEntityDTO>> uniqueAccountPricingData) {
        DatabaseServerTierCostDTO.Builder dbTierDTOBuilder = DatabaseServerTierCostDTO.newBuilder();
        for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
            for (TopologyEntityDTO region: regions) {
                DatabaseServerPriceBundle priceBundle = marketCloudRateExtractor.getDatabaseServerPriceBundle(tier.getOid(),
                        region.getOid(), accountPricingData);

                Map<Long, Map<String, DatabaseServerPrice>> dbsPriceMap = Maps.newHashMap();
                priceBundle.getPrices().forEach(price ->
                        dbsPriceMap.computeIfAbsent(price.getAccountId(), ba -> Maps.newHashMap())
                                .computeIfAbsent(price.toString(), id -> price));

                Map<String, DatabaseServerPrice> pricesForBa = dbsPriceMap.get(accountPricingData.getAccountPricingDataOid());
                if (pricesForBa == null) {
                    logger.debug("Cost not found for dbs tier {}::{} - region {} - AccountPricingDataOid {}",
                            tier.getDisplayName(), tier.getTypeSpecificInfo().getDatabaseServerTier().getStorageTier(),
                            region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
                    continue;
                }
                final Set<CommodityType> licenseCommoditySet = getLicenseCommodities(tier);
                for (CommodityType licenseCommodity : licenseCommoditySet) {
                    double price = Double.POSITIVE_INFINITY;
                    String licenseId = licenseCommodity.getKey();
                    // Keeps the storage options of the Azure DB tiers
                    List<DbsStorageOption> storageOptions = new ArrayList<>();
                    // we support just LicenseIncluded and NoLicenseRequired
                    if (licenseId.contains(LICENSE_MODEL_MAP.get(LicenseModel.LICENSE_INCLUDED))
                            || licenseId.contains(LICENSE_MODEL_MAP.get(LicenseModel.NO_LICENSE_REQUIRED))) {
                        licenseId = normalizeLicenseId(licenseId);
                        // lookup for license without LicenseModel in the priceMap
                        DatabaseServerPrice databasePrice = pricesForBa.get(licenseId);
                        if (databasePrice != null) {
                            price = databasePrice.getHourlyPrice();
                            storageOptions.addAll(databasePrice.getStorageOptions());
                        } else {
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

                    final Multimap<Integer, DependentResourceOption> storageOptionMap = ArrayListMultimap.create();
                    for (DbsStorageOption storageOption : storageOptions) {
                        storageOptionMap.put(storageOption.getCommodityType(), convertStorageOptionToResourceOption(storageOption));
                    }
                    final List<DependentCostTuple> dependentCostTuples = new ArrayList<>();
                    for (Integer commodityType : storageOptionMap.keySet()) {
                        final Collection<DependentResourceOption> dependentResourceOptions =
                                storageOptionMap.get(commodityType);
                        final int dependentResourceType = commodityConverter.commoditySpecification(
                                CommodityType.newBuilder().setType(commodityType).build())
                                .getType();
                        dependentCostTuples.add(
                                DependentCostTuple.newBuilder()
                                        .setDependentResourceType(dependentResourceType)
                                        .addAllDependentResourceOptions(dependentResourceOptions)
                                        .build());
                    }
                    CommoditySpecificationTO spec =
                            commodityConverter.commoditySpecification(licenseCommodity);
                    dbTierDTOBuilder.addCostTupleList(
                            createCostTupleWithDependent(accountPricingData.getAccountPricingDataOid(), spec.getType(), price, region.getOid(),
                                    dependentCostTuples));
                }
                // price when license isn't available
                dbTierDTOBuilder.addCostTupleList(CostTuple.newBuilder()
                        .setBusinessAccountId(accountPricingData.getAccountPricingDataOid())
                        .setLicenseCommodityType(-1)
                        .setRegionId(region.getOid())
                        .setPrice(Double.POSITIVE_INFINITY));
            }
        }

        return CostDTO.newBuilder()
                .setDatabaseServerTierCost(dbTierDTOBuilder.setLicenseCommodityBaseType(
                        CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .build())
                .build();
    }

    /**
     * Create the CBTP cost dto.
     *
     * @param reservedInstanceKey reservedInstanceKey of the RI for which the CostDTO is created.
     * @param businessAccountIdByAccountPricingData The accountPricing to use for the specific business account.
     * @param region The region the RIDiscountedMarketTier is being created for.
     * @param computeTiersInScope The compute tier in scope of the RI.
     * @param applicableBusinessAccounts account scope of the cbtp.
     *
     * @return The CBTP cost dto.
     */
    CostDTO createCbtpCostDTO(final ReservedInstanceKey reservedInstanceKey,
                              Multimap<AccountPricingData<TopologyEntityDTO>, Long> businessAccountIdByAccountPricingData,
                              TopologyEntityDTO region,
                              Set<TopologyEntityDTO> computeTiersInScope,
                              final Set<Long> applicableBusinessAccounts) {
        Set<CostTuple> costTupleList = new HashSet<>();
        Set<AccountPricingData<TopologyEntityDTO>> accountPricingDatasInScope = new HashSet<>();
        for (Map.Entry<AccountPricingData<TopologyEntityDTO>, Long> entry: businessAccountIdByAccountPricingData.entries()) {
            if (applicableBusinessAccounts.contains(entry.getValue())) {
                accountPricingDatasInScope.add(entry.getKey());
            }
        }
        for (final AccountPricingData accountPricingData: accountPricingDatasInScope) {
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
        final Collection<Long> scope;
        if (reservedInstanceKey.getShared()) {
            scope = reservedInstanceKey.getAccountScopeId();
        } else if (!applicableBusinessAccounts.isEmpty()) {
            scope = applicableBusinessAccounts;
        } else {
            logger.error("Empty applicable accounts list for " + reservedInstanceKey);
            scope = reservedInstanceKey.getAccountScopeId();
        }

        return CostDTO.newBuilder()
                .setCbtpResourceBundle(CbtpCostDTO.newBuilder()
                        .setCouponBaseType(CommodityDTO.CommodityType.COUPON_VALUE)
                        .addAllCostTupleList(costTupleList)
                        .setDiscountPercentage(1)
                        .setLicenseCommodityBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .addAllScopeIds(scope))
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
                                                                 final AccountPricingData<TopologyEntityDTO> accountPricingData,
                                                                 final TopologyEntityDTO region,
                                                                 final Set<TopologyEntityDTO> computeTiersInScope) {
        Map<OSType, CostTuple.Builder> costTupleBuilderByOs = new HashMap<>();
        Map<OSType, Double> maxComputePriceByOS = new HashMap<>();

        // For each compute tier in scope of the RI, we need to send the reserved license rates
        // as part of the cost tuple. We also iterator over the compute tiers, selecting the tier with the
        // largest cost as the basis for the nominal fee of the CBPT (if there are no associated reserved
        // license rates to differentiate the RIs)
        for (TopologyEntityDTO computeTier : computeTiersInScope) {

            final ComputePriceBundle computePriceBundle = marketCloudRateExtractor.getComputePriceBundle(computeTier,
                    region.getOid(), accountPricingData);

            computePriceBundle.getPrices().forEach(computePrice -> {
                if (computePrice.hourlyPrice() > maxComputePriceByOS.getOrDefault(computePrice.osType(), 0.0)) {
                    maxComputePriceByOS.put(computePrice.osType(), computePrice.hourlyPrice());
                }
            });

            final Set<CoreBasedLicensePriceBundle> reservedLicensePriceBundles =
                    marketCloudRateExtractor.getReservedLicensePriceBundles(accountPricingData, computeTier)
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
                                            Set<AccountPricingData<TopologyEntityDTO>> uniqueAccountPricingData) {
        CostDTO.StorageTierCostDTO.Builder storageDTO =  StorageTierCostDTO.newBuilder();
        // Mapping from commodityType to StorageResourceCost builder.
        Map<CommodityType, StorageTierCostDTO.StorageResourceCost.Builder> commType2ResourceCostBuilderMap
                = new HashMap<>();
        for (TopologyEntityDTO region: connectedRegions) {
            for (AccountPricingData accountPricingData : uniqueAccountPricingData) {
                StoragePriceBundle storageCostBundle = marketCloudRateExtractor
                        .getStoragePriceBundle(tier.getOid(), region.getOid(), accountPricingData);
                if (storageCostBundle == null) {
                    // This can happen in unit tests
                    logger.warn("Failed to get pricing information for tier {} on region {}",
                            tier.getDisplayName(), region.getDisplayName());
                    return CostDTO.newBuilder().setStorageTierCost(storageDTO.build()).build();
                }
                tier.getCommoditySoldListList().forEach(c -> {
                    CommodityType commType = c.getCommodityType();
                    List<StorageTierPriceData> priceDataList = storageCostBundle.getPrices(commType);
                    if (!priceDataList.isEmpty()) {
                        StorageTierCostDTO.StorageResourceCost.Builder builder =
                                commType2ResourceCostBuilderMap.computeIfAbsent(commType,
                                        k -> StorageTierCostDTO.StorageResourceCost.newBuilder()
                                                .setResourceType(commodityConverter.commoditySpecification(commType)));
                        priceDataList.forEach(p -> builder.addStorageTierPriceData(p));
                    }
                });
            }
        }
        // StorageTier CostDTO contains one StorageResourceCost object for one commodityType,
        // and each StorageResourceCost object contains price data including all regions and accounts combinations.
        commType2ResourceCostBuilderMap.values().forEach(b -> storageDTO.addStorageResourceCost(b.build()));
        tier.getCommoditySoldListList().forEach(c -> {
            CommodityType commType = c.getCommodityType();
            // populates the min and max capacity for a given commodity
            if (c.hasMaxAmountForConsumer() && c.hasMinAmountForConsumer()) {
                storageDTO.addStorageResourceLimitation(StorageTierCostDTO
                        .StorageResourceLimitation.newBuilder()
                        .setResourceType(commodityConverter.commoditySpecification(commType))
                        .setMaxCapacity(c.getMaxAmountForConsumer())
                        .setMinCapacity(c.getMinAmountForConsumer())
                        .setCheckMinCapacity(c.getCheckMinAmountForConsumer())
                        .build());
            }
            // populates the ratio dependency between max this commodity and its base commodity
            if (c.hasRatioDependency()) {
                StorageResourceRatioDependency.Builder ratioBuilder = StorageTierCostDTO
                        .StorageResourceRatioDependency.newBuilder()
                        .setBaseResourceType(commodityConverter
                                .commoditySpecification(c.getRatioDependency().getBaseCommodity()))
                        .setDependentResourceType(commodityConverter.commoditySpecification(commType))
                        .setMaxRatio(c.getRatioDependency().getMaxRatio());
                if (c.getRatioDependency().hasMinRatio()) {
                    ratioBuilder.setMinRatio(c.getRatioDependency().getMinRatio());
                }
                if (c.getRatioDependency().hasIncreaseBaseAmountDefaultSupported()) {
                    ratioBuilder.setIncreaseBaseDefaultSupported(
                            c.getRatioDependency().getIncreaseBaseAmountDefaultSupported());
                }
                storageDTO.addStorageResourceRatioDependency(ratioBuilder.build());
            }
            // populates the ranged capacity which is dependent on base commodity
            if (c.hasRangeDependency()) {
                StorageResourceRangeDependency.Builder storageResourceRangeBuilder =
                        StorageTierCostDTO.StorageResourceRangeDependency.newBuilder()
                                .setBaseResourceType(commodityConverter
                                        .commoditySpecification(TopologyDTO.CommodityType.newBuilder()
                                                .setType(c.getRangeDependency().getBaseCommodity().getNumber())
                                                .build()))
                                .setDependentResourceType(commodityConverter.commoditySpecification(commType));
                for (CommodityDTO.RangeTuple rangeTuple : c.getRangeDependency().getRangeTupleList()) {
                    storageResourceRangeBuilder.addRangeTuple(RangeTuple.newBuilder()
                            .setBaseMaxCapacity(rangeTuple.getBaseMaxAmountForConsumer())
                            .setDependentMaxCapacity(rangeTuple.getDependentMaxAmountForConsumer())
                            .build());
                }
                storageDTO.addStorageResourceRangeDependency(storageResourceRangeBuilder.build());
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

                    if (netThruPut != null && ioThruPut != null && netThruPut.size() == 1
                            && ioThruPut.size() == 1) {
                        ComputeResourceDependency.Builder dependency = ComputeResourceDependency
                                .newBuilder()
                                .setBaseResourceType(commodityConverter.commoditySpecification(
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


    /**
     * Get license commodities for the tier.
     *
     * @param tier tier topology DTO
     * @return license commodities for the tier
     */
    protected static Set<CommodityType> getLicenseCommodities(TopologyEntityDTO tier) {
        Set<CommodityType> licenseCommoditySet = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .collect(Collectors.toSet());
        return licenseCommoditySet;
    }

    /**
     * Normalize licenseId by removing license model.
     *
     * @param licenseId license Identifier
     * @return Normalize license Identifier
     */
    @Nonnull
    protected static String normalizeLicenseId(@Nonnull String licenseId) {
        String newLicenseId = licenseId.replace(LICENSE_MODEL_MAP.get(LicenseModel.LICENSE_INCLUDED), "");
        return newLicenseId.replace(LICENSE_MODEL_MAP.get(LicenseModel.NO_LICENSE_REQUIRED), "");
    }

}
