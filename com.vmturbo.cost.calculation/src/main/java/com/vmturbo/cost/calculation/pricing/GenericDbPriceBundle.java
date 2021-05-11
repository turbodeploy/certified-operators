package com.vmturbo.cost.calculation.pricing;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.cost.calculation.pricing.GenericDbPriceBundle.GenericPrice;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

/**
 * A generic bundle of of possible prices for a (database/database server tier, region)
 * combination. The possible prices are affected by the {@link DatabaseEngine} and owning
 * business account of the VM consuming from the tier.
 *
 * @param <T> generic price type
 */
public class GenericDbPriceBundle<T extends GenericPrice> {

    private static final Map<DatabaseEngine, String> DB_ENGINE_MAP =
            ImmutableMap.<DatabaseEngine, String>builder().put(DatabaseEngine.AURORAMYSQL, "AuroraMySQL")
                    .put(DatabaseEngine.AURORAPOSTGRESQL, "AuroraPostgreSQL")
                    .put(DatabaseEngine.MARIADB, "MariaDb")
                    .put(DatabaseEngine.MYSQL, "MySql")
                    .put(DatabaseEngine.ORACLE, "Oracle")
                    .put(DatabaseEngine.POSTGRESQL, "PostgreSql")
                    .put(DatabaseEngine.SQLSERVER, "SqlServer")
                    .put(DatabaseEngine.UNKNOWN, "Unknown")
                    .build();

    private static final Map<DatabaseEdition, String> DB_EDITION_MAP =
            ImmutableMap.<DatabaseEdition, String>builder().put(DatabaseEdition.ENTERPRISE,
                    "Enterprise").put(DatabaseEdition.STANDARD, "Standard").put(
                    DatabaseEdition.STANDARDONE, "Standard One").put(DatabaseEdition.STANDARDTWO,
                    "Standard Two").put(DatabaseEdition.WEB, "Web").put(DatabaseEdition.EXPRESS,
                    "Express").build();

    private static final Map<DeploymentType, String> DEPLOYMENT_TYPE_MAP =
            ImmutableMap.<DeploymentType, String>builder().put(DeploymentType.MULTI_AZ, "MultiAz")
                    .put(DeploymentType.SINGLE_AZ, "SingleAz")
                    .build();

    private final List<T> prices;

    protected GenericDbPriceBundle(@Nonnull final List<T> prices) {
        this.prices = Objects.requireNonNull(prices);
    }

    /**
     * Get the list of database prices.
     *
     * @return A list of database prices.
     */
    @Nonnull
    public List<T> getPrices() {
        return prices;
    }

    /**
     * Temporary generic object for integration with CostTuples.
     * In the future it may be good to have the {@link CloudRateExtractor} return cost tuples
     * directly.
     *
     * @param <O> storage options type
     */
    public static class GenericPrice<O extends GenericStorageOption> {
        private final long accountId;
        private final DatabaseEngine dbEngine;
        private final DatabaseEdition dbEdition;
        private final DeploymentType depType;
        private final LicenseModel licenseModel;
        private final double hourlyPrice;
        private final List<O> storageOptions;

        /**
         * Constructor for the database price.
         *
         * @param accountId The account id.
         * @param dbEngine The db engine.
         * @param dbEdition The db edition.
         * @param depType The deployment type.
         * @param licenseModel The license model.
         * @param hourlyPrice The hourly price.
         * @param storageOptions The storage options
         */
        public GenericPrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                @Nonnull final DatabaseEdition dbEdition, @Nullable final DeploymentType depType,
                @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                @Nonnull final List<O> storageOptions) {
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
        public List<O> getStorageOptions() {
            return storageOptions;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other instanceof GenericPrice) {
                GenericPrice otherPrice = (GenericPrice)other;
                return accountId == otherPrice.accountId && dbEngine == otherPrice.dbEngine
                        && hourlyPrice == otherPrice.hourlyPrice;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, dbEngine, dbEdition, depType, licenseModel, hourlyPrice);
        }

        @Override
        public String toString() {
            // skip licenseModel while converting databasePrice to string
            return DB_ENGINE_MAP.getOrDefault(this.getDbEngine(), "null") + ":"
                    + DB_EDITION_MAP.getOrDefault(this.getDbEdition(), "null") + ":"
                    // we filter out all the multi-az deploymentTypes in the cost-probe. We will not find any cost for a license containing multi-az.
                    + ((this.getDeploymentType() != null && DEPLOYMENT_TYPE_MAP.get(
                            this.getDeploymentType()) != null) ? DEPLOYMENT_TYPE_MAP.get(
                            this.getDeploymentType()) : "null") + ":";
            // skip licenseModel while converting databasePrice to string
        }
    }

    /**
     * Class representing a generic storage option.
     */
    public static class GenericStorageOption {

        private final long endRange;
        private final double price;
        private final long increment;

        /**
         * Possible increment in GB in this option (ex. 250GB).
         *
         * @return possible increment in GB in this option
         */
        public long getIncrement() {
            return increment;
        }


        /**
         * The end range of the commodity in commodity units.
         * E.g. for Storage amount that is GB.
         *
         * @return end range of the commodity in commodity units
         */
        public long getEndRange() {
            return endRange;
        }

        /**
         * This is the price per commodity unit.
         * E.g. 0.2 per GB
         *
         * @return the price per commodity unit
         */
        public double getPrice() {
            return price;
        }

        /**
         * Constructor for the storage option.
         *
         * @param increment the increment.
         * @param endRange The end range.
         * @param price The price.
         */
        public GenericStorageOption(long increment, long endRange, double price) {
            this.increment = increment;
            this.endRange = endRange;
            this.price = price;
        }
    }
}
