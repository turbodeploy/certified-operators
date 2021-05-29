package com.vmturbo.cost.calculation.pricing;

import static com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit.GB_MONTH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.DbServerTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.DbTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle.DatabasePrice.StorageOption;
import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle.DatabaseServerPrice.DbsStorageOption;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList.DatabaseServerTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * The {@link CloudRateExtractor} is a wrapper around a {@link com.vmturbo.common.protobuf.cost.Pricing.PriceTable}
 * and other pricing information. The purpose is to allow lookups of prices of different configurations.
 */
public class CloudRateExtractor {

    private static final Logger logger = LogManager.getLogger();

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor;

    private static final Set<String> TIERS_WITH_PRIORITY = ImmutableSet.of("IO2");

    private static final Multimap<String, Unit> TIER2UNITS_ACCUMULATIVE_COST_MAP
            = new ImmutableSetMultimap.Builder<String, Unit>()
                .putAll("GP3", Unit.MBPS_MONTH, Unit.MILLION_IOPS)
                .build();

    /**
     * A mapping between the OS string indicated by the license access commodity key
     * and the corresponding {@link CloudCostDTO.OSType}.
     */
    public static final BiMap<String, OSType> OS_TYPE_MAP = ImmutableBiMap.<String, CloudCostDTO.OSType>builder()
            .put("Linux", OSType.LINUX)
            .put("RHEL", OSType.RHEL)
            .put("SUSE", OSType.SUSE)
            .put("UNKNOWN", OSType.UNKNOWN_OS)
            .put("Windows", OSType.WINDOWS)
            .put("Windows_SQL_Standard", OSType.WINDOWS_WITH_SQL_STANDARD)
            .put("Windows_SQL_Web", OSType.WINDOWS_WITH_SQL_WEB)
            .put("Windows_SQL_Server_Enterprise", OSType.WINDOWS_WITH_SQL_ENTERPRISE)
            .put("Windows_Bring_your_own_license", OSType.WINDOWS_BYOL)
            .put("Linux_SQL_Server_Enterprise", OSType.LINUX_WITH_SQL_ENTERPRISE)
            .put("Linux_SQL_Standard", OSType.LINUX_WITH_SQL_STANDARD)
            .put("Linux_SQL_Web", OSType.LINUX_WITH_SQL_WEB)
            .build();

    /**
     * Constructor for the price table.
     *
     * @param cloudTopology The cloud topology.
     * @param entityInfoExtractor The entity info extractor.
     */
    public CloudRateExtractor(@Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor) {
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.entityInfoExtractor = Objects.requireNonNull(entityInfoExtractor);
    }

    /**
     * Get the {@link ComputePriceBundle} listing the possible configuration and account-dependent
     * prices for a compute tier in a specific region.
     *
     * @param tier The compute tier.
     * @param regionId The ID of the region.
     * @param accountPricingData The account specific pricing data.
     *
     * @return A {@link ComputePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link CloudRateExtractor} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle.
     */
    @Nonnull
    public ComputePriceBundle getComputePriceBundle(final TopologyEntityDTO tier, final long regionId,
            @Nonnull final AccountPricingData<TopologyEntityDTO> accountPricingData) {
        long tierId = tier.getOid();
        final ComputePriceBundle.Builder priceBuilder = ComputePriceBundle.newBuilder();

        if (accountPricingData == null || accountPricingData.getPriceTable() == null) {
            return priceBuilder.build();
        }

        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId, accountPricingData);
        if (regionPriceTable == null) {
            return priceBuilder.build();
        }

        ComputeTierPriceList computeTierPrices = regionPriceTable.getComputePricesByTierIdMap().get(tierId);
        if (computeTierPrices == null) {
            logger.debug("Price list not found for tier {} in region {}'s price table."
                            + " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, regionId);
            return priceBuilder.build();
        }

        final OSType baseOsType = computeTierPrices.getBasePrice().getGuestOsType();
        final double baseHourlyPrice =
                computeTierPrices.getBasePrice().getPricesList().get(0).getPriceAmount().getAmount();

        DiscountApplicator<TopologyEntityDTO> discountApplicator = accountPricingData.getDiscountApplicator();
        entityInfoExtractor.getComputeTierConfig(tier).ifPresent(computeTierConfig -> {
            final double discount = (1.0 - discountApplicator.getDiscountPercentage(tierId).getValue());
            final double basePriceWithDiscount = baseHourlyPrice * discount;
            // for each OS - calculate its license price and save its total price
            tier.getCommoditySoldListList().stream()
                    .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                    .map(CommoditySoldDTO::getCommodityType)
                    .map(licenceCommodityType -> OS_TYPE_MAP.get(licenceCommodityType.getKey()))
                    .forEach(osType -> {
                        // let cost calculation component figure out the correct
                        // license price that should be added to the base price
                        final LicensePriceTuple licensePrice = accountPricingData.getLicensePrice(
                                computeTierConfig, osType, computeTierPrices);
                        final double totalLicensePrice = licensePrice.getImplicitOnDemandLicensePrice() * discount
                                + licensePrice.getExplicitOnDemandLicensePrice();
                        priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), osType,
                                basePriceWithDiscount, totalLicensePrice, osType == baseOsType);
                    });
        });
        return priceBuilder.build();
    }

    /**
     * Get the {@link DatabasePriceBundle} listing the possible configuration and account-dependent
     * prices for a database tier in a specific region.
     *
     * @param tierId The ID of the compute tier.
     * @param regionId The ID of the region.
     * @param accountPricingData The account specific pricing data.
     *
     * @return A {@link DatabasePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link CloudRateExtractor} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle
     */
    @Nonnull
    public DatabasePriceBundle getDatabasePriceBundle(final long tierId, final long regionId,
            @Nonnull final AccountPricingData<TopologyEntityDTO> accountPricingData) {

        final DatabasePriceBundle.Builder priceBuilder = DatabasePriceBundle.newBuilder();

        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId, accountPricingData);
        if (regionPriceTable == null) {
            return priceBuilder.build();
        }

        DbTierOnDemandPriceTable dbTierPriceTable =
                regionPriceTable.getDbPricesByInstanceIdMap().get(tierId);
        if (dbTierPriceTable == null) {
            logger.debug("Price list not found for tier {} in region {}'s price table."
                            + " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, regionId);
            return priceBuilder.build();
        }

        List<DatabaseTierPriceList> dbTierPriceLists = new ArrayList<>();

        if (dbTierPriceTable.hasOnDemandPricesNoDeploymentType()) {
            dbTierPriceLists.add(dbTierPriceTable.getOnDemandPricesNoDeploymentType());
        }

        dbTierPriceLists.addAll(dbTierPriceTable.getDbPricesByDeploymentTypeMap().values());

        for (DatabaseTierPriceList dbTierPrices : dbTierPriceLists) {
            addDbTierPricesToPriceTable(dbTierPrices, tierId, accountPricingData, priceBuilder);
        }


        return priceBuilder.build();
    }

    /**
     * Get the {@link DatabaseServerPriceBundle} listing the possible configuration and account-dependent
     * prices for a database server tier in a specific region.
     *
     * @param tierId The ID of the database server tier.
     * @param regionId The ID of the region.
     * @param accountPricingData The account specific pricing data.
     *
     * @return A {@link DatabaseServerPriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link CloudRateExtractor} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle
     */
    @Nonnull
    public DatabaseServerPriceBundle getDatabaseServerPriceBundle(final long tierId, final long regionId,
            @Nonnull final AccountPricingData<TopologyEntityDTO> accountPricingData) {
        final DatabaseServerPriceBundle.Builder priceBuilder = DatabaseServerPriceBundle.newBuilder();

        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId, accountPricingData);
        if (regionPriceTable == null) {
            logger.debug("Price table is not found for tier {} in region {}", tierId, regionId);
            return priceBuilder.build();
        }
        DbServerTierOnDemandPriceTable dbsTierPriceTable =
                regionPriceTable.getDbsPricesByInstanceIdMap().get(tierId);
        if (dbsTierPriceTable == null) {
            logger.debug("Price list not found for database server tier {} in region {}'s price table."
                            + " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, regionId);
            return priceBuilder.build();
        }

        for (DatabaseServerTierPriceList priceList: dbsTierPriceTable.getDbsPricesByTierIdMap().values()) {
            addDbsTierPricesToPriceBundle(priceList, tierId, accountPricingData, priceBuilder);
        }

        return priceBuilder.build();
    }

    private void addDbTierPricesToPriceTable(DatabaseTierPriceList dbTierPrices, long tierId,
            AccountPricingData accountPricingData,
            DatabasePriceBundle.Builder priceBuilder) {
        final DatabaseTierConfigPrice dbTierBasePrice = dbTierPrices.getBasePrice();
        final DatabaseEdition dbEdition = dbTierBasePrice.getDbEdition();
        final DatabaseEngine dbEngine = dbTierBasePrice.getDbEngine();
        final DeploymentType deploymentType = dbTierPrices.hasDeploymentType()
                ? dbTierPrices.getDeploymentType() : null;
        final LicenseModel licenseModel = dbTierBasePrice.hasDbLicenseModel()
                ? dbTierBasePrice.getDbLicenseModel() : null;
        if (dbTierBasePrice.getPricesList().size() == 0) {
            logger.error("The base template for DB does not have any price. Ignoring DB tier `{}`.",
                    tierId);
            return;
        }
        final double baseHourlyPrice =
                CostProtoUtil.getHourlyPriceAmount(dbTierBasePrice.getPricesList().get(0));
        DiscountApplicator<TopologyEntityDTO> discountApplicator =
                accountPricingData.getDiscountApplicator();
        // Add storage options- Azure DTU-based DB
        List<StorageOption> storageOptions = new ArrayList<>();
        for (Price dependentPrice : dbTierPrices.getDependentPricesList()) {
            storageOptions.add(new StorageOption(dependentPrice.getIncrementInterval(),
                    dependentPrice.getEndRangeInUnits(),
                    CostProtoUtil.getHourlyPriceAmount(dependentPrice)));
        }
        /*
        Sort storage options ascending based on the end range. Turbonomic needs to find the least
        expensive option that can fit the demand. This is why we sort the options based on the
        end range.
         */
        Collections.sort(storageOptions,
                (a, b) -> Long.valueOf(a.getEndRange() - b.getEndRange()).intValue());
        // Add the base configuration price.
        priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), dbEngine, dbEdition,
                deploymentType, licenseModel, baseHourlyPrice
                        * (1.0 - discountApplicator.getDiscountPercentage(tierId).getValue()),
                storageOptions);

        for (DatabaseTierConfigPrice dbTierConfigPrice : dbTierPrices.getConfigurationPriceAdjustmentsList()) {

            if (dbTierConfigPrice.getPricesList().size() == 0) {
                logger.warn("There is no price associated with "
                        + dbTierConfigPrice.getDbEngine() + ":"
                        + dbTierConfigPrice.getDbEdition() + ":"
                        + deploymentType + ":"
                        + (dbTierConfigPrice.hasDbLicenseModel()
                        ? dbTierConfigPrice.getDbLicenseModel() : null));
                continue;
            }
            priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(),
                    dbTierConfigPrice.getDbEngine(), dbTierConfigPrice.getDbEdition(),
                    deploymentType,
                    dbTierConfigPrice.hasDbLicenseModel() ? dbTierConfigPrice.getDbLicenseModel()
                            : null, (baseHourlyPrice
                            + dbTierConfigPrice.getPricesList().get(0).getPriceAmount().getAmount())
                            * (1.0 - discountApplicator.getDiscountPercentage(tierId).getValue()),
                    storageOptions);

        }
    }

    private void addDbsTierPricesToPriceBundle(DatabaseServerTierPriceList dbTierPrices, long tierId,
            AccountPricingData<TopologyEntityDTO> accountPricingData,
            DatabaseServerPriceBundle.Builder priceBuilder) {

        for (DatabaseServerTierConfigPrice serverConfigPrice : dbTierPrices.getConfigPricesList()) {
            final DatabaseTierConfigPrice configPrice =
                    serverConfigPrice.getDatabaseTierConfigPrice();

            final DatabaseEdition dbEdition = configPrice.getDbEdition();
            final DatabaseEngine dbEngine = configPrice.getDbEngine();
            final DeploymentType deploymentType =
                    configPrice.hasDbDeploymentType() ? configPrice.getDbDeploymentType() : null;
            final LicenseModel licenseModel =
                    configPrice.hasDbLicenseModel() ? configPrice.getDbLicenseModel() : null;
            if (configPrice.getPricesList().isEmpty()) {
                logger.error(
                        "The base template for DB server does not have any price. Ignoring DB tier `{}`.",
                        tierId);
                return;
            }

            final double baseHourlyPrice = CostProtoUtil.getHourlyPriceAmount(
                    configPrice.getPricesList().get(0));
            DiscountApplicator<TopologyEntityDTO> discountApplicator =
                    accountPricingData.getDiscountApplicator();

            final List<DbsStorageOption> storageOptions = new ArrayList<>();
            serverConfigPrice.getDependentPricesList().forEach(dependentPrice -> {
                switch (dependentPrice.getUnit()) {
                    case GB_MONTH:
                        storageOptions.add(new DbsStorageOption(dependentPrice.getIncrementInterval(),
                                dependentPrice.getEndRangeInUnits(),
                                CostProtoUtil.getHourlyPriceAmount(dependentPrice), CommodityType.STORAGE_AMOUNT_VALUE));
                        break;
                    case MILLION_IOPS:
                        storageOptions.add(new DbsStorageOption(dependentPrice.getIncrementInterval(),
                                dependentPrice.getEndRangeInUnits(),
                                CostProtoUtil.getHourlyPriceAmount(dependentPrice), CommodityType.STORAGE_ACCESS_VALUE));
                    default:
                        // other not yet supported.
                }
            });

            storageOptions.sort((a, b) -> Long.valueOf(a.getEndRange() - b.getEndRange()).intValue());
            // Add the base configuration price.
            final double hourlyPrice = baseHourlyPrice * (1.0 - discountApplicator.getDiscountPercentage(tierId)
                    .getValue());
            priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), dbEngine,
                    dbEdition, deploymentType, licenseModel, hourlyPrice, storageOptions,
                    serverConfigPrice.getRatioDependecyList());
        }
    }

    @Nullable
    OnDemandPriceTable getOnDemandPriceTable(final long tierId, final long regionId, AccountPricingData accountPricingData) {
        if (!cloudTopology.getEntity(tierId).isPresent()) {
            logger.error("Tier {} not found in topology. Returning empty price bundle.", tierId);
            return null;
        }
        final OnDemandPriceTable regionPriceTable = accountPricingData.getPriceTable()
                .getOnDemandPriceByRegionIdMap().get(regionId);
        if (regionPriceTable == null) {
            logger.warn("On-Demand price table not found for region {}."
                    + " Cost data might not have been uploaded yet.", regionId);
            return null;
        }
        return regionPriceTable;
    }

    /**
     * Get the {@link StoragePriceBundle} listing the possible configuration and account-dependent
     * prices for a storage tier in a specific region.
     *
     * @param tierId The ID of the storage tier.
     * @param regionId The ID of the region.
     * @param accountPricingData The accountPricingData object containing pricing information for a business account.
     * @return A {@link StoragePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link CloudRateExtractor} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price bundle.
     */
    @Nonnull
    public StoragePriceBundle getStoragePriceBundle(final long tierId, final long regionId, final AccountPricingData accountPricingData) {
        final StoragePriceBundle.Builder priceBuilder = StoragePriceBundle.newBuilder();
        if (accountPricingData == null || accountPricingData.getPriceTable() == null) {
            return priceBuilder.build();
        }
        final Optional<TopologyEntityDTO> storageTierOpt = cloudTopology.getEntity(tierId);
        if (!storageTierOpt.isPresent()) {
            logger.error("Tier {} not found in topology. Returning empty price bundle.", tierId);
            return priceBuilder.build();
        } else if (!cloudTopology.getEntity(regionId).isPresent()) {
            logger.error("Region {} not found in topology. Returning empty price bundle.", regionId);
            return priceBuilder.build();
        }

        // For storage, the storage tier sells access (IOPS), amount (GB), throughput (MBPS) commodities.
        // We first find the commodity types and collect them so we can provide prices for them.
        final Set<TopologyDTO.CommodityType> soldAccessTypes = Sets.newHashSet();
        final Set<TopologyDTO.CommodityType> soldAmountTypes = Sets.newHashSet();
        final Set<TopologyDTO.CommodityType> soldThroughputTypes = Sets.newHashSet();
        final TopologyEntityDTO storageTier = storageTierOpt.get();
        for (final CommoditySoldDTO commSold : storageTier.getCommoditySoldListList()) {
            switch (commSold.getCommodityType().getType()) {
                case CommodityType.STORAGE_ACCESS_VALUE:
                    soldAccessTypes.add(commSold.getCommodityType());
                    break;
                case CommodityType.STORAGE_AMOUNT_VALUE:
                    soldAmountTypes.add(commSold.getCommodityType());
                    break;
                case CommodityType.IO_THROUGHPUT_VALUE:
                    soldThroughputTypes.add(commSold.getCommodityType());
                    break;
                default:
                    break;
            }
        }

        if (soldAccessTypes.isEmpty()) {
            logger.warn("Storage tier {} (id: {}) not selling any storage access commodities. "
                    + "Not able to provide IOPS price bundles.", storageTier.getDisplayName(), tierId);
        }
        if (soldAmountTypes.isEmpty()) {
            logger.warn("Storage tier {} (id: {}) not selling any storage amount commodities. "
                    + "Not able to provide GB price bundles.", storageTier.getDisplayName(), tierId);
        }
        if (soldThroughputTypes.isEmpty()) {
            logger.warn("Storage tier {} (id: {}) not selling any IO Throughput commodities. "
                    + "Not able to provide MBPS price bundles.", storageTier.getDisplayName(), tierId);
        }

        final OnDemandPriceTable regionPriceTable = accountPricingData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
        if (regionPriceTable == null) {
            logger.warn("On-Demand price table not found for region {}."
                    + " Cost data might not have been uploaded yet.", regionId);
            return priceBuilder.build();
        }

        final StorageTierPriceList tierPriceList =
                regionPriceTable.getCloudStoragePricesByTierIdMap().get(tierId);
        if (tierPriceList == null) {
            logger.warn("Price list not found for tier {} in region {}'s price table."
                            + " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, regionId);
            return priceBuilder.build();
        }

        // Some tiers within the same region provides the same price and we would like to define their priority.
        // For example, IO1 and IO2 tiers have the same pricing, and IO2 can provide higher durability and larger
        // IOPS capability. Currently, analysis engine makes decision based on price, and when two tiers provide
        // the same price, a random one is chosen.
        // Decrease a tiny value for the price of the tier with higher priority, so analysis engine prefers it.
        // The decrease is tiny enough comparing with the actual price. Math.ulp(1.0) = 2.220446049250313E-16
        final double tinyDecreaseForPreferredTiers
                = TIERS_WITH_PRIORITY.contains(storageTier.getDisplayName()) ? Math.ulp(1.0) : 0;

        DiscountApplicator<TopologyEntityDTO> discountApplicator = accountPricingData.getDiscountApplicator();
        tierPriceList.getCloudStoragePriceList().forEach(storagePrice -> {
            // Group the prices by unit. The unit will determine whether a price is for the
            // "access" (IOPS), "amount" (GB) or "throughput" (MBPS) commodities sold by the storage tier.
            final Map<Price.Unit, List<Price>> pricesByUnit =
                    storagePrice.getPricesList().stream()
                            .collect(Collectors.groupingBy(Price::getUnit));
            pricesByUnit.forEach((unit, priceList) -> {
                // Accumulative cost for GP3 IOPS and Throughput.
                final boolean isAccumulativePrice = TIER2UNITS_ACCUMULATIVE_COST_MAP.containsEntry(storageTier.getDisplayName(), unit);

                // A price is considered a "unit" price if the amount of units consumed
                // affects the price.
                final boolean isUnitPrice = unit == GB_MONTH
                        || unit == Unit.MILLION_IOPS || unit == Unit.MBPS_MONTH
                        || unit == Unit.IO_REQUESTS;

                // Based on the unit, determine the commodity types the prices are for.
                // The same prices apply to all commodities of the same type - the price table
                // does not make any distinction based on commodity key.
                final Set<TopologyDTO.CommodityType> soldCommTypes;
                switch (unit) {
                    case MILLION_IOPS:
                    case IO_REQUESTS:
                        soldCommTypes = soldAccessTypes;
                        break;
                    case MBPS_MONTH:
                        soldCommTypes = soldThroughputTypes;
                        break;
                    default:
                        soldCommTypes = soldAmountTypes;
                }
                priceList.forEach(price -> {
                    final double discountPercentage = discountApplicator.getDiscountPercentage(tierId).getValue();

                    // Note: We probably don't need to normalize to hours in month because currently
                    // storage prices are monthly. But it's technically possible to get hourly
                    // storage price, so we do this to be safe.
                    double hourlyPriceAmount = CostProtoUtil.getHourlyPriceAmount(price);
                    if (hourlyPriceAmount > tinyDecreaseForPreferredTiers) {
                        hourlyPriceAmount -= tinyDecreaseForPreferredTiers;
                    }

                    final StorageTierPriceData.Builder priceDataBuilder = StorageTierPriceData.newBuilder()
                            .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountPricingData.getAccountPricingDataOid()).setRegionId(regionId)
                                    .setPrice(hourlyPriceAmount * (1 - discountPercentage)).build())
                            .setIsUnitPrice(isUnitPrice)
                            .setIsAccumulativeCost(isAccumulativePrice)
                            .setAppliedToHistoricalQuantity(unit == Unit.IO_REQUESTS);

                    // Even in an accumulative price, the last price will have no valid end range.
                    if (price.getEndRangeInUnits() > 0 && price.getEndRangeInUnits() < Long.MAX_VALUE) {
                        priceDataBuilder.setUpperBound(price.getEndRangeInUnits());
                    } else {
                        priceDataBuilder.setUpperBound(Double.POSITIVE_INFINITY);
                    }

                    final StorageTierPriceData priceData = priceDataBuilder.build();
                    soldCommTypes.forEach(soldCommType ->
                            priceBuilder.addPrice(soldCommType, priceData));
                });
            });
        });

        return priceBuilder.build();
    }

    /**
     * Get the reserved license price bundles from the price table for a given tier.
     *
     * @param accountPricingData The account pricing data.
     * @param tier The cloud tier.
     *
     * @return A set of core based license price bundles.
     */
    public Set<CoreBasedLicensePriceBundle> getReservedLicensePriceBundles(@Nonnull AccountPricingData accountPricingData,
            @Nonnull TopologyEntityDTO tier) {


        final DiscountApplicator<TopologyEntityDTO> discountApplicator = accountPricingData.getDiscountApplicator();
        final double discount = (1.0 - discountApplicator.getDiscountPercentage(tier).getValue());

        final int numCores = tier.getTypeSpecificInfo().getComputeTier().getNumCores();
        final boolean burstableCPU = tier.getTypeSpecificInfo().getComputeTier().getBurstableCPU();

        final Optional<TopologyDTO.CommodityType> vcoreCommodityTime = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.NUM_VCORE_VALUE)
                .map(CommoditySoldDTO::getCommodityType)
                .findAny();


        return entityInfoExtractor.getComputeTierConfig(tier).map(computeTierConfig ->
                tier.getCommoditySoldListList().stream()
                        .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .map(CommoditySoldDTO::getCommodityType)
                        .map(licenseCommodityType -> {

                            final OSType osType = OS_TYPE_MAP.get(licenseCommodityType.getKey());

                            final Optional<CurrencyAmount> reservedLicensePrice =
                                    accountPricingData.getReservedLicensePrice(computeTierConfig, osType);

                            final Optional<Double> discountedPrice = reservedLicensePrice.map(CurrencyAmount::getAmount)
                                    .map(fullPrice -> fullPrice * discount);


                            final ImmutableCoreBasedLicensePriceBundle.Builder bundleBuilder =
                                    ImmutableCoreBasedLicensePriceBundle.builder()
                                            .osType(osType)
                                            .licenseCommodityType(licenseCommodityType)
                                            .numCores(numCores)
                                            .isBurstableCPU(burstableCPU);

                            vcoreCommodityTime.ifPresent(bundleBuilder::vcoreCommodityType);
                            discountedPrice.ifPresent(bundleBuilder::price);

                            return bundleBuilder.build();
                        }).collect(Collectors.<CoreBasedLicensePriceBundle>toSet()))

                .orElse(Collections.emptySet());
    }

    /**
     * A factory class for creating {@link CloudRateExtractor} instances.
     */
    public static class CloudRateExtractorFactory {

        /**
         * Creates a new rate extractor.
         * @param cloudTopology The cloud topology containing referenced cloud tiers.
         * @param entityInfoExtractor The entity info extractor.
         * @return The newly constructed rate extractor.
         */
        @Nonnull
        public CloudRateExtractor newRateExtractor(@Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                                                   @Nonnull final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor) {
            return new CloudRateExtractor(cloudTopology, entityInfoExtractor);
        }
    }

    /**
     * A bundle of of possible prices for a (storage tier, region) combination. The possible
     * prices are affected by the business account of the VM consuming from the tier, as well
     * as the amount of storage or IOPS being consumed.
     */
    public static class StoragePriceBundle {

        private final Map<TopologyDTO.CommodityType, List<StorageTierPriceData>> pricesByCommType;

        private StoragePriceBundle(
                @Nonnull final Map<TopologyDTO.CommodityType, List<StorageTierPriceData>> pricesByCommType) {
            this.pricesByCommType = Objects.requireNonNull(pricesByCommType);
        }

        /**
         * Get the {@link StorageTierPriceData} contained in the bundle.
         *
         * @param commType The commodity type.
         *
         * @return The list of {@link StorageTierPriceData}.
         */
        @Nonnull
        public List<StorageTierPriceData> getPrices(TopologyDTO.CommodityType commType) {
            return pricesByCommType.getOrDefault(commType, Collections.emptyList());
        }

        /**
         * Get the builder.
         *
         * @return The builder.
         */
        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Custom builder for the Storage prices.
         */
        public static class Builder {
            private final Map<TopologyDTO.CommodityType, List<StorageTierPriceData>> pricesByCommType
                    = new HashMap<>();

            private Builder() {}

            /**
             * Add a price for a comm type.
             *
             * @param commType The commodity type.
             * @param price The price to be added for the commodity.
             *
             * @return The builder.
             */
            @Nonnull
            public Builder addPrice(@Nonnull final TopologyDTO.CommodityType commType,
                    @Nonnull final StorageTierPriceData price) {
                pricesByCommType.compute(commType, (k, existingPrices) -> {
                    final List<StorageTierPriceData> updatedList = existingPrices == null
                            ? new ArrayList<>() : existingPrices;
                    updatedList.add(price);
                    return updatedList;
                });
                return this;
            }

            /**
             * Build the Storage Price bundle.
             *
             * @return The Storage price bundle.
             */
            @Nonnull
            public StoragePriceBundle build() {
                // The individual lists will still be modifiable, but it's not worth the effort
                // make them unmodifiable. No one cares anyway.
                return new StoragePriceBundle(Collections.unmodifiableMap(pricesByCommType));
            }
        }
    }

    /**
     * A bundle of of possible prices for a (compute tier, region) combination. The possible
     * prices are affected by the {@link OSType} and owning business account of the VM consuming
     * from the tier.
     */
    public static class ComputePriceBundle {

        private final List<ComputePrice> prices;

        private ComputePriceBundle(@Nonnull final List<ComputePrice> prices) {
            this.prices = Objects.requireNonNull(prices);
        }

        @Nonnull
        public List<ComputePrice> getPrices() {
            return prices;
        }

        /**
         * Get a builder.
         *
         * @return The builder.
         */
        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * A builder for the compute price bundle.
         */
        public static class Builder {
            private final ImmutableList.Builder<ComputePrice> priceBuilder = ImmutableList.builder();

            private Builder() {}

            /**
             * Add a price to the compute price builder.
             *
             * @param accountId The account id.
             * @param osType The operating system.
             * @param computePrice The price for compute.
             * @param licensePrice The license price for {@code osType}.
             * @param isBasePrice True if price is for linux.
             *
             * @return The builder.
             */
            @Nonnull
            public Builder addPrice(long accountId,
                                    OSType osType,
                                    double computePrice,
                                    double licensePrice,
                                    boolean isBasePrice) {
                // TODO (roman, September 25) - Replace with CostTuple
                priceBuilder.add(ComputePrice.builder()
                        .accountId(accountId)
                        .osType(osType)
                        .hourlyComputeRate(computePrice)
                        .hourlyLicenseRate(licensePrice)
                        .isBasePrice(isBasePrice)
                        .build());
                return this;
            }

            /**
             * Build the compute prices.
             *
             * @return A compute price bundle.
             */
            @Nonnull
            public ComputePriceBundle build() {
                return new ComputePriceBundle(priceBuilder.build());
            }
        }

        /**
         * Temporary object for integration with CostTuples.
         * In the future it may be good to have the {@link CloudRateExtractor} return cost tuples
         * directly.
         */
        @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
        @Immutable
        public interface ComputePrice {

            /**
             * The account ID (really the pricing data OID).
             * @return The account ID
             */
            long accountId();

            /**
             * The platform.
             * @return The platform.
             */
            @Nonnull
            OSType osType();

            /**
             * The hourly compute rate.
             * @return The hourly compute rate.
             */
            double hourlyComputeRate();

            /**
             * The hourly license rate associated with {@link #osType()}.
             * @return The hourly license rate associated with {@link #osType()}.
             */
            double hourlyLicenseRate();

            /**
             * Whether this price is the lowest rate for the compute tier.
             * @return Whether this price is the lowest rate for the compute tier.
             */
            boolean isBasePrice();

            /**
             * The hourly rate encompassing the compute and license rates.
             * @return The hourly rate encompassing the compute and license rates.
             */
            @Derived
            default double hourlyPrice() {
                return hourlyComputeRate() + hourlyLicenseRate();
            }

            /**
             * Constructs and returns a new {@link Builder} instance.
             * @return The newly constructed builder instance.
             */
            @Nonnull
            static Builder builder() {
                return new Builder();
            }

            /**
             * A builder class for constructing {@link ComputePrice} instances.
             */
            class Builder extends ImmutableComputePrice.Builder {}
        }
    }

    /**
     * A pricing bundle for core-based license rates.
     */
    @Immutable
    public interface CoreBasedLicensePriceBundle {

        /**
         * The license commodity type of the license bundle.
         * @return The license commodity type
         */
        TopologyDTO.CommodityType licenseCommodityType();

        /**
         * The vcore commodity type of the license rate. If the vcore commodity type is empty, it
         * indicates the compute tier does not sell the appropriate commodity to have core-based license
         * rates properly calculated in the market.
         * @return The {@link CommodityType#NUM_VCORE} commodity type or {@link Optional#empty()},
         * if the associated tier does not sell the commodity.
         */
        Optional<TopologyDTO.CommodityType> vcoreCommodityType();

        /**
         * The {@link OSType} of the license bundle.
         * @return The {@link OSType} of the license bundle.
         */
        OSType osType();

        /**
         * The number of core for the license bundle.
         * @return The number of core for the license bundle.
         */
        int numCores();

        /**
         * Indicates whether the license bundle is for burstable CPU tiers.
         * @return True, if the associated tier supports burstable CPU. False otherwise.
         */
        boolean isBurstableCPU();

        /**
         * The price for the license bundle. If there is no rate for the configuration (tier, os, core
         * count), the price will be empty. In this case, an empty price is used to indicate support
         * for the {@link OSType}.
         * @return The price for the license bundle.
         */
        Optional<Double> price();

        /**
         * Checks whether {@link #price()} is set.
         * @return A boolean indicating whether {@link #price()} is set.
         */
        @Derived
        default boolean hasPrice() {
            return price().isPresent();
        }
    }
}
