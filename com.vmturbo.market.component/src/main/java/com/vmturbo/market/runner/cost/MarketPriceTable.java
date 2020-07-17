package com.vmturbo.market.runner.cost;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.DbTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice.StorageOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * The {@link MarketPriceTable} is a wrapper around a {@link com.vmturbo.common.protobuf.cost.Pricing.PriceTable}
 * and other pricing information. The purpose is to allow lookups of prices of different configurations.
 */
public class MarketPriceTable {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCostData cloudCostData;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor;

    public static final Map<DatabaseEngine, String> DB_ENGINE_MAP = ImmutableMap.<DatabaseEngine, String>builder()
            .put(DatabaseEngine.AURORA, "Aurora")
            .put(DatabaseEngine.MARIADB, "MariaDb")
            .put(DatabaseEngine.MYSQL, "MySql")
            .put(DatabaseEngine.ORACLE, "Oracle")
            .put(DatabaseEngine.POSTGRESQL, "PostgreSql")
            .put(DatabaseEngine.SQLSERVER, "SqlServer")
            .put(DatabaseEngine.UNKNOWN, "Unknown").build();

    public static final Map<DatabaseEdition, String> DB_EDITION_MAP = ImmutableMap.<DatabaseEdition, String>builder()
            .put(DatabaseEdition.ENTERPRISE, "Enterprise")
            .put(DatabaseEdition.STANDARD, "Standard")
            .put(DatabaseEdition.STANDARDONE, "Standard One")
            .put(DatabaseEdition.STANDARDTWO, "Standard Two")
            .put(DatabaseEdition.WEB, "Web")
            .put(DatabaseEdition.EXPRESS, "Express").build();

    public static final Map<DeploymentType, String> DEPLOYMENT_TYPE_MAP = ImmutableMap.<DeploymentType, String>builder()
            .put(DeploymentType.MULTI_AZ, "MultiAz")
            .put(DeploymentType.SINGLE_AZ, "SingleAz").build();

    public static final Map<LicenseModel, String> LICENSE_MODEL_MAP = ImmutableMap.<LicenseModel, String>builder()
            .put(LicenseModel.BRING_YOUR_OWN_LICENSE, "BringYourOwnLicense")
            .put(LicenseModel.LICENSE_INCLUDED, "LicenseIncluded")
            .put(LicenseModel.NO_LICENSE_REQUIRED, "NoLicenseRequired").build();

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
            .put("Windows_Server", OSType.WINDOWS_SERVER)
            .put("Windows_Server_Burst", OSType.WINDOWS_SERVER_BURST)
            .put("Linux_SQL_Server_Enterprise", OSType.LINUX_WITH_SQL_ENTERPRISE)
            .put("Linux_SQL_Standard", OSType.LINUX_WITH_SQL_STANDARD)
            .put("Linux_SQL_Web", OSType.LINUX_WITH_SQL_WEB)
            .build();


    MarketPriceTable(@Nonnull final CloudCostData cloudCostData,
                            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                            @Nonnull final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor) {
        this.cloudCostData = Objects.requireNonNull(cloudCostData);
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
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle.
     */
    @Nonnull
    public ComputePriceBundle getComputePriceBundle(final TopologyEntityDTO tier, final long regionId,
                                                    final AccountPricingData accountPricingData) {
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
            logger.warn("Price list not found for tier {} in region {}'s price table." +
                " Cost data might not have been uploaded, or the tier is not available in the region.",
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
                        final LicensePriceTuple licensePrice = accountPricingData.getLicensePrice(osType,
                            computeTierConfig.getNumCores(), computeTierPrices, computeTierConfig.isBurstableCPU());
                        final double totalLicensePrice = licensePrice.getImplicitOnDemandLicensePrice() * discount
                            + licensePrice.getExplicitOnDemandLicensePrice();
                        priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), osType,
                            basePriceWithDiscount + totalLicensePrice, osType == baseOsType);
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
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle.
     */
    @Nonnull
    public DatabasePriceBundle getDatabasePriceBundle(final long tierId, final long regionId,
                                                      final AccountPricingData accountPricingData) {
        final DatabasePriceBundle.Builder priceBuilder = DatabasePriceBundle.newBuilder();
        if (accountPricingData == null || accountPricingData.getPriceTable() == null) {
            logger.error("No account pricing data found for business account oid {}");
            return priceBuilder.build();
        }
        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId, accountPricingData);
        if (regionPriceTable == null) {
            return priceBuilder.build();
        }

        DbTierOnDemandPriceTable dbTierPriceTable =
            regionPriceTable.getDbPricesByInstanceIdMap().get(tierId);
        if (dbTierPriceTable == null) {
            logger.warn("Price list not found for tier {} in region {}'s price table." +
                " Cost data might not have been uploaded, or the tier is not available in the region.",
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

    private void addDbTierPricesToPriceTable(DatabaseTierPriceList dbTierPrices, long tierId,
                                             AccountPricingData accountPricingData,
                                             DatabasePriceBundle.Builder priceBuilder) {
        final DatabaseTierConfigPrice dbTierBasePrice = dbTierPrices.getBasePrice();
        final DatabaseEdition dbEdition = dbTierBasePrice.getDbEdition();
        final DatabaseEngine dbEngine = dbTierBasePrice.getDbEngine();
        final DeploymentType deploymentType = dbTierPrices.hasDeploymentType() ?
            dbTierPrices.getDeploymentType() : null;
        final LicenseModel licenseModel = dbTierBasePrice.hasDbLicenseModel() ?
            dbTierBasePrice.getDbLicenseModel() : null;
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
        // Add the base configuration price.
        priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), dbEngine, dbEdition,
                deploymentType, licenseModel, baseHourlyPrice *
                        (1.0 - discountApplicator.getDiscountPercentage(tierId).getValue()),
                storageOptions);

        for (DatabaseTierConfigPrice dbTierConfigPrice : dbTierPrices.getConfigurationPriceAdjustmentsList()) {

            if (dbTierConfigPrice.getPricesList().size() == 0) {
                logger.warn("There is no price associated with "
                    + dbTierConfigPrice.getDbEngine() + ":"
                    + dbTierConfigPrice.getDbEdition() + ":"
                    + deploymentType + ":"
                    + (dbTierConfigPrice.hasDbLicenseModel() ?
                    dbTierConfigPrice.getDbLicenseModel() : null));
                continue;
            }
            priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(),
                    dbTierConfigPrice.getDbEngine(), dbTierConfigPrice.getDbEdition(),
                    deploymentType,
                    dbTierConfigPrice.hasDbLicenseModel() ? dbTierConfigPrice.getDbLicenseModel() :
                            null, (baseHourlyPrice +
                            dbTierConfigPrice.getPricesList().get(0).getPriceAmount().getAmount()) *
                            (1.0 - discountApplicator.getDiscountPercentage(tierId).getValue()),
                    storageOptions);
        }
    }

    @Nullable
    OnDemandPriceTable getOnDemandPriceTable(final long tierId, final long regionId, AccountPricingData accountPricingData) {
        if (!cloudTopology.getEntity(tierId).isPresent()) {
            logger.error("Tier {} not found in topology. Returning empty price bundle.", tierId);
            return null;
        } else if (!cloudTopology.getEntity(regionId).isPresent()) {
            logger.error("Region {} not found in topology. Returning empty price bundle.", regionId);
            return null;
        }

        final OnDemandPriceTable regionPriceTable = accountPricingData.getPriceTable()
                .getOnDemandPriceByRegionIdMap().get(regionId);
        if (regionPriceTable == null) {
            logger.warn("On-Demand price table not found for region {}." +
                    " Cost data might not have been uploaded yet.", regionId);
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
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price bundle.
     */
    @Nonnull
    public StoragePriceBundle getStoragePriceBundle(final long tierId, final long regionId, final AccountPricingData accountPricingData) {
        final StoragePriceBundle.Builder priceBuilder = StoragePriceBundle.newBuilder();
        if (accountPricingData == null || accountPricingData.getPriceTable() == null) {
            logger.error("No account pricing data found for business account oid {}");
            return null;
        }
        final Optional<TopologyEntityDTO> storageTierOpt = cloudTopology.getEntity(tierId);
        if (!storageTierOpt.isPresent()) {
            logger.error("Tier {} not found in topology. Returning empty price bundle.", tierId);
            return priceBuilder.build();
        } else if (!cloudTopology.getEntity(regionId).isPresent()) {
            logger.error("Region {} not found in topology. Returning empty price bundle.", regionId);
            return priceBuilder.build();
        }

        // For storage, the storage tier sells access (IOPS) and amount (GB) commodities.
        // We first find the commodity types and collect them so we can provide prices for them.
        final Set<TopologyDTO.CommodityType> soldAccessTypes = Sets.newHashSet();
        final Set<TopologyDTO.CommodityType> soldAmountTypes = Sets.newHashSet();
        final TopologyEntityDTO storageTier = storageTierOpt.get();
        for (final CommoditySoldDTO commSold : storageTier.getCommoditySoldListList()) {
            if (commSold.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE) {
                soldAccessTypes.add(commSold.getCommodityType());
            } else if (commSold.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE) {
                soldAmountTypes.add(commSold.getCommodityType());
            }
        }

        if (soldAccessTypes.isEmpty()) {
            logger.warn("Storage tier {} (id: {}) not selling any storage access commodities. " +
                "Not able to provide IOPS price bundles.", storageTier.getDisplayName(), tierId);
        }
        if (soldAmountTypes.isEmpty()) {
            logger.warn("Storage tier {} (id: {}) not selling any storage amount commodities. " +
                    "Not able to provide GB price bundles.", storageTier.getDisplayName(), tierId);
        }

        final OnDemandPriceTable regionPriceTable = accountPricingData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
        if (regionPriceTable == null) {
            logger.warn("On-Demand price table not found for region {}." +
                    " Cost data might not have been uploaded yet.", regionId);
            return priceBuilder.build();
        }

        final StorageTierPriceList tierPriceList =
                regionPriceTable.getCloudStoragePricesByTierIdMap().get(tierId);
        if (tierPriceList == null) {
            logger.warn("Price list not found for tier {} in region {}'s price table." +
                            " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, regionId);
            return priceBuilder.build();
        }
        DiscountApplicator<TopologyEntityDTO> discountApplicator = accountPricingData.getDiscountApplicator();
        tierPriceList.getCloudStoragePriceList().forEach(storagePrice -> {
            // Group the prices by unit. The unit will determine whether a price is for the
            // "access" (IOPS) or "amount" (GB) commodities sold by the storage tier.
            final Map<Price.Unit, List<Price>> pricesByUnit =
                storagePrice.getPricesList().stream()
                    .collect(Collectors.groupingBy(Price::getUnit));
            pricesByUnit.forEach((unit, priceList) -> {
                // Each price in the price list is considered "accumulative" if the price list
                // contains different costs for different ranges. It's called "accumulative"
                // because to get the total price we need to "accumulate" the prices for the
                // individual ranges.
                final boolean isAccumulativePrice = priceList.stream()
                        .anyMatch(price -> price.getEndRangeInUnits() > 0 &&
                                price.getEndRangeInUnits() < Long.MAX_VALUE);

                // A price is considered a "unit" price if the amount of units consumed
                // affects the price.
                final boolean isUnitPrice = unit == Unit.GB_MONTH || unit == Unit.MILLION_IOPS;

                // Based on the unit, determine the commodity types the prices are for.
                // The same prices apply to all commodities of the same type - the price table
                // does not make any distinction based on commodity key.
                final Set<TopologyDTO.CommodityType> soldCommTypes =
                        unit == Unit.MILLION_IOPS ? soldAccessTypes : soldAmountTypes;
                priceList.forEach(price -> {
                    final double discountPercentage = discountApplicator.getDiscountPercentage(tierId).getValue();

                    // Note: We probably don't need to normalize to hours in month because currently
                    // storage prices are monthly. But it's technically possible to get hourly
                    // storage price, so we do this to be safe.
                    final double hourlyPriceAmount = CostProtoUtil.getHourlyPriceAmount(price);

                    final StorageTierPriceData.Builder priceDataBuilder = StorageTierPriceData.newBuilder()
                            .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(accountPricingData.getAccountPricingDataOid()).setRegionId(regionId)
                                    .setPrice(hourlyPriceAmount * (1 - discountPercentage)).build())
                            .setIsUnitPrice(isUnitPrice)
                            .setIsAccumulativeCost(isAccumulativePrice);

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
     * Gets the Reserved Instance Prices for a given tier and region condensed into a ComputeTierPrice
     * bundle.
     *
     * @param accountPricingData The account specific pricing data.
     * @param region The region to get the pricing for.
     * @param tier The compute tier to get the pricing for.
     *
     * @return The ComputeTierPriceBundle.
     */
    public ComputePriceBundle getReservedLicensePriceBundle(AccountPricingData accountPricingData, TopologyEntityDTO region,
                                                TopologyEntityDTO tier) {
        final ComputePriceBundle.Builder priceBuilder = ComputePriceBundle.newBuilder();
        if (accountPricingData == null) {
            logger.debug("Account Pricing data not found.");
            return priceBuilder.build();
        }
        long tierId = tier.getOid();
        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, region.getOid(), accountPricingData);

        if (regionPriceTable == null) {
            logger.debug("No prices found for region {}, for account pricing data with id {}",
                    region.getDisplayName(), accountPricingData.getAccountPricingDataOid());
            return priceBuilder.build();
        }

        ComputeTierPriceList computeTierPrices = regionPriceTable.getComputePricesByTierIdMap().get(tierId);
        if (computeTierPrices == null) {
            logger.debug("Price list not found for tier {} in region {}'s price table." +
                            " Cost data might not have been uploaded, or the tier is not available in the region.",
                    tierId, region.getDisplayName());
            return priceBuilder.build();
        }
        final OSType baseOsType = computeTierPrices.getBasePrice().getGuestOsType();
        entityInfoExtractor.getComputeTierConfig(tier).ifPresent(computeTierConfig -> {
                tier.getCommoditySoldListList().stream()
                        .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .map(CommoditySoldDTO::getCommodityType)
                        .map(licenceCommodityType -> OS_TYPE_MAP.get(licenceCommodityType.getKey()))
                        .forEach(osType -> {
                            // Get the license price tuple and get the reserved license price.
                            final LicensePriceTuple licensePrice = accountPricingData.getLicensePrice(osType,
                                    computeTierConfig.getNumCores(), computeTierPrices, computeTierConfig.isBurstableCPU());
                            double reservedLicensePrice = licensePrice.getReservedInstanceLicensePrice();
                            logger.debug("Found reserved instance license price {} for region {} and tier {}" +
                                    " and account pricing data oid {}", reservedLicensePrice, region.getDisplayName(),
                                    tier.getDisplayName(), accountPricingData.getAccountPricingDataOid());
                            priceBuilder.addPrice(accountPricingData.getAccountPricingDataOid(), osType,
                                    reservedLicensePrice, osType == baseOsType);
                        });
        });
        return priceBuilder.build();
    }


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

                        final Optional<LicensePrice> reservedLicensePrice =
                                accountPricingData.getReservedLicensePrice(osType, numCores, burstableCPU);

                        final Optional<Double> discountedPrice = reservedLicensePrice.map(LicensePrice::getPrice)
                                .map(Price::getPriceAmount)
                                .map(CurrencyAmount::getAmount)
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
         * @return The list of {@link StorageTierPriceData}.
         */
        @Nonnull
        public List<StorageTierPriceData> getPrices(TopologyDTO.CommodityType commType) {
            return pricesByCommType.getOrDefault(commType, Collections.emptyList());
        }

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private final Map<TopologyDTO.CommodityType, List<StorageTierPriceData>> pricesByCommType
                    = new HashMap<>();

            private Builder() {}

            @Nonnull
            public Builder addPrice(@Nonnull final TopologyDTO.CommodityType commType,
                                    @Nonnull final StorageTierPriceData price) {
                pricesByCommType.compute(commType, (k, existingPrices) -> {
                    final List<StorageTierPriceData> updatedList = existingPrices == null ?
                            new ArrayList<>() : existingPrices;
                    updatedList.add(price);
                    return updatedList;
                });
                return this;
            }

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

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private final ImmutableList.Builder<ComputePrice> priceBuilder = ImmutableList.builder();

            private Builder() {}

            @Nonnull
            public Builder addPrice(final long accountId, final OSType osType,
                                    final double hourlyPrice, final boolean isBasePrice) {
                // TODO (roman, September 25) - Replace with CostTuple
                priceBuilder.add(new ComputePrice(accountId, osType, hourlyPrice, isBasePrice));
                return this;
            }

            @Nonnull
            public ComputePriceBundle build() {
                return new ComputePriceBundle(priceBuilder.build());
            }
        }


        /**
         * Temporary object for integration with CostTuples.
         *
         * In the future it may be good to have the {@link MarketPriceTable} return cost tuples
         * directly.
         */
        public static class ComputePrice {
            private final long accountId;
            private final OSType osType;
            private final double hourlyPrice;
            private final boolean isBasePrice;

            public ComputePrice(final long accountId,
                                final OSType osType,
                                final double hourlyPrice,
                                final boolean isBasePrice) {
                this.accountId = accountId;
                this.osType = osType;
                this.hourlyPrice = hourlyPrice;
                this.isBasePrice = isBasePrice;
            }

            public long getAccountId() {
                return accountId;
            }

            public OSType getOsType() {
                return osType;
            }

            public double getHourlyPrice() {
                return hourlyPrice;
            }

            public boolean isBasePrice() {
                return isBasePrice;
            }

            @Override
            public boolean equals(Object other) {
                if (other == this) return true;
                if (other instanceof ComputePrice) {
                    ComputePrice otherPrice = (ComputePrice)other;
                    return accountId == otherPrice.accountId &&
                            osType == otherPrice.osType &&
                            hourlyPrice == otherPrice.hourlyPrice &&
                            isBasePrice == otherPrice.isBasePrice;
                } else {
                    return false;
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(accountId, osType, hourlyPrice, isBasePrice);
            }
        }
    }

    /**
     * A bundle of of possible prices for a (database tier, region) combination. The possible
     * prices are affected by the {@link DatabaseEngine} and owning business account of the VM consuming
     * from the tier.
     */
    public static class DatabasePriceBundle {

        private final List<DatabasePrice> prices;

        private DatabasePriceBundle(@Nonnull final List<DatabasePrice> prices) {
            this.prices = Objects.requireNonNull(prices);
        }

        @Nonnull
        public List<DatabasePrice> getPrices() {
            return prices;
        }

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private final ImmutableList.Builder<DatabasePrice> priceBuilder = ImmutableList.builder();

            private Builder() {}

            @Nonnull
            public Builder addPrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                    @Nonnull final DatabaseEdition dbEdition,
                    @Nullable final DeploymentType depType,
                    @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                    @Nonnull final List<StorageOption> storageOptions) {
                // TODO (roman, September 25) - Replace with CostTuple
                priceBuilder.add(
                        new DatabasePrice(accountId, dbEngine, dbEdition, depType, licenseModel,
                                hourlyPrice, storageOptions));
                return this;
            }

            @Nonnull
            public DatabasePriceBundle build() {
                return new DatabasePriceBundle(priceBuilder.build());
            }
        }

        /**
         * Temporary object for integration with CostTuples.
         *
         * In the future it may be good to have the {@link MarketPriceTable} return cost tuples
         * directly.
         */
        public static class DatabasePrice {
            private final long accountId;
            private final DatabaseEngine dbEngine;
            private final DatabaseEdition dbEdition;
            private final DeploymentType depType;
            private final LicenseModel licenseModel;
            private final double hourlyPrice;
            private final List<StorageOption> storageOptions;

            public DatabasePrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                    @Nonnull final DatabaseEdition dbEdition,
                    @Nullable final DeploymentType depType,
                    @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                    @Nonnull final List<StorageOption> storageOptions) {
                this.accountId = accountId;
                this.dbEngine = dbEngine;
                this.dbEdition = dbEdition;
                this.depType = depType;
                this.licenseModel = licenseModel;
                this.hourlyPrice = hourlyPrice;
                this.storageOptions = storageOptions;
            }

            public long getAccountId() {
                return accountId;
            }

            public DatabaseEngine getDbEngine() {
                return dbEngine;
            }

            public DatabaseEdition getDbEdition() {
                return dbEdition;
            }

            public DeploymentType getDeploymentType() {
                return depType;
            }

            public LicenseModel getLicenseModel() {
                return licenseModel;
            }

            /**
             * This returns the hourly price of the base commodity type.
             *
             * @return The hourly price of the base commodity type
             */
            public double getHourlyPrice() {
                return hourlyPrice;
            }

            /**
             * This returns the storage options and their prices. These options have start,
             * increment and end. The start of each option is the end range of the previous option
             * (sorted by the end range). Each storage option can be obtained by adding multiples of
             * increment to the start. Note that the final number can't be greater than end range.
             * If the number is greater than the end range, we need to consider another option. For
             * example, if we need 730GB and the option is: increment:250GB, end_range:750GB,
             * start:500GB, we need to add one increment to the 500GB (start). The right option for
             * this demand is 750GB since 730GB is less than or equals this number and this is
             * obtainable by adding the increment to the start.
             *
             * @return The storage options and their prices
             */
            public List<StorageOption> getStorageOptions() {
                return storageOptions;
            }

            public static class StorageOption {
                // This is the possible increment in GB in this option (ex. 250GB).
                final long increment;
                // This is the end range in GB in this option (ex. 500GB).
                final long endRange;
                // This is the price per GB in this option (ex. 0.2 per GB).
                final double price;

                public long getIncrement() {
                    return increment;
                }

                public long getEndRange() {
                    return endRange;
                }

                public double getPrice() {
                    return price;
                }

                public StorageOption(long increment, long endRange, double price) {
                    this.increment = increment;
                    this.endRange = endRange;
                    this.price = price;
                }
            }

            @Override
            public boolean equals(Object other) {
                if (other == this) return true;
                if (other instanceof DatabasePrice) {
                    DatabasePrice otherPrice = (DatabasePrice)other;
                    return accountId == otherPrice.accountId &&
                            dbEngine == otherPrice.dbEngine &&
                            hourlyPrice == otherPrice.hourlyPrice;
                } else {
                    return false;
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(accountId, dbEngine, dbEdition,
                        depType, licenseModel, hourlyPrice);
            }

            public String toString() {
                // skip licenseModel while converting databasePrice to string
                return (DB_ENGINE_MAP.get(this.getDbEngine()) != null ? DB_ENGINE_MAP.get(this.getDbEngine()) : "null") + ":" +
                        (DB_EDITION_MAP.get(this.getDbEdition()) != null ? DB_EDITION_MAP.get(this.getDbEdition()) : "null") + ":" +
                        // we filter out all the multi-az deploymentTypes in the cost-probe. We will not find any cost for a license containing multi-az.
                        ((this.getDeploymentType() != null && DEPLOYMENT_TYPE_MAP.get(this.getDeploymentType()) != null) ? DEPLOYMENT_TYPE_MAP.get(this.getDeploymentType()) : "null") + ":";
            }
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
    }
}
