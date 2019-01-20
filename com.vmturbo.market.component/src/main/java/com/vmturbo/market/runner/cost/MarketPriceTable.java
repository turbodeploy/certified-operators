package com.vmturbo.market.runner.cost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
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

    public static ImmutableMap<DatabaseEngine, String> DB_ENGINE_MAP = ImmutableMap.<DatabaseEngine, String>builder()
            .put(DatabaseEngine.AURORA, "Aurora")
            .put(DatabaseEngine.MARIADB, "MariaDb")
            .put(DatabaseEngine.MYSQL, "MySql")
            .put(DatabaseEngine.ORACLE, "Oracle")
            .put(DatabaseEngine.POSTGRESQL, "PostgreSql")
            .put(DatabaseEngine.SQL_SERVER, "SqlServer")
            .put(DatabaseEngine.UNKNOWN, "Unknown").build();

    public static ImmutableMap<DatabaseEdition, String> DB_EDITION_MAP = ImmutableMap.<DatabaseEdition, String>builder()
            .put(DatabaseEdition.ORACLE_ENTERPRISE, "Enterprise")
            .put(DatabaseEdition.ORACLE_STANDARD, "Standard")
            .put(DatabaseEdition.ORACLE_STANDARD_1, "Standard One")
            .put(DatabaseEdition.ORACLE_STANDARD_2, "Standard Two")
            .put(DatabaseEdition.SQL_SERVER_ENTERPRISE, "Enterprise")
            .put(DatabaseEdition.SQL_SERVER_STANDARD, "Standard")
            .put(DatabaseEdition.SQL_SERVER_WEB, "Web")
            .put(DatabaseEdition.SQL_SERVER_EXPRESS, "Express").build();

    public static ImmutableMap<DeploymentType, String> DEPLOYMENT_TYPE_MAP = ImmutableMap.<DeploymentType, String>builder()
            .put(DeploymentType.MULTI_AZ, "MultiAz")
            .put(DeploymentType.SINGLE_AZ, "SingleAz").build();

    public static ImmutableMap<LicenseModel, String> LICENSE_MODEL_MAP = ImmutableMap.<LicenseModel, String>builder()
            .put(LicenseModel.BRING_YOUR_OWN_LICENSE, "BringYourOwnLicense")
            .put(LicenseModel.LICENSE_INCLUDED, "LicenseIncluded")
            .put(LicenseModel.NO_LICENSE_REQUIRED, "NoLicenseRequired").build();


    /**
     * The {@link DiscountApplicator}s associated with each business account in the cloud topology.
     */
    private final Map<Long, DiscountApplicator<TopologyEntityDTO>> discountsByBusinessAccount;

    MarketPriceTable(@Nonnull final CloudCostData cloudCostData,
                            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                            @Nonnull final EntityInfoExtractor<TopologyEntityDTO> entityInfoExtractor,
                            @Nonnull final DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory) {
        this.cloudCostData = Objects.requireNonNull(cloudCostData);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.discountsByBusinessAccount = Collections.unmodifiableMap(cloudTopology.getEntities().values().stream()
            .filter(entity -> entity.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toMap(Function.identity(), accountId ->
                discountApplicatorFactory.accountDiscountApplicator(accountId,
                    cloudTopology, entityInfoExtractor, cloudCostData))));
    }

    /**
     * Get the {@link ComputePriceBundle} listing the possible configuration and account-dependent
     * prices for a compute tier in a specific region.
     *
     * @param tierId The ID of the compute tier.
     * @param regionId The ID of the region.
     * @return A {@link ComputePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle.
     */
    @Nonnull
    public ComputePriceBundle getComputePriceBundle(final long tierId, final long regionId) {
        final ComputePriceBundle.Builder priceBuilder = ComputePriceBundle.newBuilder();

        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId);
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
        final Tenancy baseTenancy = computeTierPrices.getBasePrice().getTenancy();
        final double baseHourlyPrice =
                computeTierPrices.getBasePrice().getPricesList().get(0).getPriceAmount().getAmount();

        discountsByBusinessAccount.forEach((accountId, discountApplicator) -> {
            // Add the base configuration price.
            priceBuilder.addPrice(accountId, baseOsType,
                    baseHourlyPrice * (1.0 - discountApplicator.getDiscountPercentage(tierId)), true);

            // Add the other configuration prices.
            computeTierPrices.getPerConfigurationPriceAdjustmentsList().stream()
                    // Ignore tenancy.
                    .filter(configPrice -> configPrice.getTenancy() == baseTenancy)
                    .filter(configPrice -> configPrice.getPricesCount() > 0)
                    .forEach(configPrice -> {
                        // For compute tiers, we assume there is exactly one price - the hourly
                        // price.
                        final double configHourlyPrice = baseHourlyPrice +
                                configPrice.getPricesList().get(0).getPriceAmount().getAmount();
                        priceBuilder.addPrice(accountId, configPrice.getGuestOsType(),
                                configHourlyPrice * (1.0 - discountApplicator.getDiscountPercentage(tierId)), false);
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
     * @return A {@link DatabasePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price
     *         bundle.
     */
    @Nonnull
    public DatabasePriceBundle getDatabasePriceBundle(final long tierId, final long regionId) {
        final DatabasePriceBundle.Builder priceBuilder = DatabasePriceBundle.newBuilder();

        OnDemandPriceTable regionPriceTable = getOnDemandPriceTable(tierId, regionId);
        if (regionPriceTable == null) {
            return priceBuilder.build();
        }

        DatabaseTierPriceList dbTierPrices = regionPriceTable.getDbPricesByInstanceIdMap().get(tierId);
        if (dbTierPrices == null) {
            logger.warn("Price list not found for tier {} in region {}'s price table." +
                " Cost data might not have been uploaded, or the tier is not available in the region.",
                tierId, regionId);
            return priceBuilder.build();
        }

        final DatabaseEdition dbEdition = dbTierPrices.getBasePrice().getDbEdition();
        final DatabaseEngine dbEngine = dbTierPrices.getBasePrice().getDbEngine();
        final DeploymentType deploymentType = dbTierPrices.getBasePrice().getDbDeploymentType();
        final LicenseModel licenseModel = dbTierPrices.getBasePrice().getDbLicenseModel();
        final double baseHourlyPrice =
                dbTierPrices.getBasePrice().getPricesList().get(0).getPriceAmount().getAmount();

        discountsByBusinessAccount.forEach((accountId, discountApplicator) -> {
            // Add the base configuration price.
            priceBuilder.addPrice(accountId, dbEngine,
                    dbEdition,
                    deploymentType,
                    licenseModel,
                    baseHourlyPrice * (1.0 - discountApplicator.getDiscountPercentage(tierId)));

            for (DatabaseTierConfigPrice dbTierConfigPrice : dbTierPrices.getConfigurationPriceAdjustmentsList()) {
                if (dbTierConfigPrice.getPricesList().size() == 0) {
                    logger.warn("There is no price associated with "
                        + dbTierConfigPrice.getDbEngine() + ":"
                        + dbTierConfigPrice.getDbEdition() + ":"
                        + dbTierConfigPrice.getDbDeploymentType() + ":"
                        + dbTierConfigPrice.getDbLicenseModel());
                    continue;
                }
                priceBuilder.addPrice(accountId,
                    dbTierConfigPrice.getDbEngine(),
                    dbTierConfigPrice.getDbEdition(),
                    dbTierConfigPrice.getDbDeploymentType(),
                    dbTierConfigPrice.getDbLicenseModel(),
                    (baseHourlyPrice + dbTierConfigPrice.getPricesList().get(0).getPriceAmount().getAmount())
                                    * (1.0 - discountApplicator.getDiscountPercentage(tierId)));
            }
        });
        return priceBuilder.build();
    }

    @Nullable
    OnDemandPriceTable getOnDemandPriceTable(final long tierId, final long regionId) {
        if (!cloudTopology.getEntity(tierId).isPresent()) {
            logger.error("Tier {} not found in topology. Returning empty price bundle.", tierId);
            return null;
        } else if (!cloudTopology.getEntity(regionId).isPresent()) {
            logger.error("Region {} not found in topology. Returning empty price bundle.", regionId);
            return null;
        }

        final OnDemandPriceTable regionPriceTable =
                cloudCostData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
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
     * @return A {@link StoragePriceBundle} of the different configurations available for this tier
     *         and region in the topology the {@link MarketPriceTable} was constructed with. If
     *         the tier or region are not found in the price table, returns an empty price bundle.
     */
    @Nonnull
    public StoragePriceBundle getStoragePriceBundle(final long tierId, final long regionId) {
        final StoragePriceBundle.Builder priceBuilder = StoragePriceBundle.newBuilder();
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

        final OnDemandPriceTable regionPriceTable =
                cloudCostData.getPriceTable().getOnDemandPriceByRegionIdMap().get(regionId);
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

        discountsByBusinessAccount.forEach((accountId, discountApplicator) -> {
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
                        final double discountPercentage = discountApplicator.getDiscountPercentage(tierId);

                        // Note: We probably don't need to normalize to hours in month because currently
                        // storage prices are monthly. But it's technically possible to get hourly
                        // storage price, so we do this to be safe.
                        final double hourlyPriceAmount = CostProtoUtil.getHourlyPriceAmount(price);

                        final StorageTierPriceData.Builder priceDataBuilder = StorageTierPriceData.newBuilder()
                                .setBusinessAccountId(accountId)
                                .setPrice(hourlyPriceAmount * (1 - discountPercentage))
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
        });

        return priceBuilder.build();
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
            public Builder addPrice(final long accountId, final DatabaseEngine dbEngine,
                    final DatabaseEdition dbEdition,
                    final DeploymentType depType,
                    final LicenseModel licenseModel,
                    final double hourlyPrice) {
                // TODO (roman, September 25) - Replace with CostTuple
                priceBuilder.add(new DatabasePrice(accountId, dbEngine, dbEdition,
                        depType, licenseModel, hourlyPrice));
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

            public DatabasePrice(final long accountId,
                                final DatabaseEngine dbEngine,
                                final DatabaseEdition dbEdition,
                                final DeploymentType depType,
                                final LicenseModel licenseModel,
                                final double hourlyPrice) {
                this.accountId = accountId;
                this.dbEngine = dbEngine;
                this.dbEdition = dbEdition;
                this.depType = depType;
                this.licenseModel = licenseModel;
                this.hourlyPrice = hourlyPrice;
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

            public double getHourlyPrice() {
                return hourlyPrice;
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
                        (DEPLOYMENT_TYPE_MAP.get(this.getDeploymentType()) != null ? DEPLOYMENT_TYPE_MAP.get(this.getDeploymentType()) : "null") + ":";
            }
        }
    }

}
