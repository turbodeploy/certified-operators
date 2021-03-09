package com.vmturbo.cost.calculation.pricing;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle.DatabaseServerPrice;
import com.vmturbo.cost.calculation.pricing.DatabaseServerPriceBundle.DatabaseServerPrice.DbsStorageOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

/**
 * A bundle of of possible prices for a (database server tier, region) combination. The possible
 * prices are affected by the {@link DatabaseEngine} and owning business account of the VM consuming
 * from the tier.
 */
public class DatabaseServerPriceBundle extends GenericDbPriceBundle<DatabaseServerPrice> {

    private DatabaseServerPriceBundle(@Nonnull final List<DatabaseServerPrice> prices) {
        super(prices);
    }

    /**
     * A builder for the Database price bundle.
     *
     * @return The builder for the database price bundle.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A static class for the DatabasePrice Builder.
     */
    public static class Builder {
        private final ImmutableList.Builder<DatabaseServerPrice> priceBuilder =
                ImmutableList.builder();

        private Builder() {}

        /**
         * Add price to db price builder.
         *
         * @param accountId The account id.
         * @param dbEngine The db engine.
         * @param dbEdition The deb edition.
         * @param depType The deployment type
         * @param licenseModel The license mode.
         * @param hourlyPrice The hourly price.
         * @param storageOptions the storage options.
         * @return A builder.
         */
        @Nonnull
        public Builder addPrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                @Nonnull final DatabaseEdition dbEdition, @Nullable final DeploymentType depType,
                @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                @Nonnull final List<DbsStorageOption> storageOptions) {
            priceBuilder.add(
                    new DatabaseServerPrice(accountId, dbEngine, dbEdition, depType, licenseModel,
                            hourlyPrice, storageOptions));
            return this;
        }

        /**
         * Build the db price bundle.
         *
         * @return The Database price bundle.
         */
        @Nonnull
        public DatabaseServerPriceBundle build() {
            return new DatabaseServerPriceBundle(priceBuilder.build());
        }
    }

    /**
     * Temporary object for integration with CostTuples.
     */
    public static class DatabaseServerPrice extends GenericDbPriceBundle.GenericPrice {

        /**
         * Constructor for the database price.
         *
         * @param accountId The account id.
         * @param dbEngine The db engine.
         * @param dbEdition The db edition.
         * @param depType The deployment type.
         * @param licenseModel The license model.
         * @param hourlyPrice The hourly price.
         * @param storageOptions The storage options.
         */
        public DatabaseServerPrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                @Nonnull final DatabaseEdition dbEdition, @Nullable final DeploymentType depType,
                @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                @Nonnull final List<DbsStorageOption> storageOptions) {
            super(accountId, dbEngine, dbEdition, depType, licenseModel, hourlyPrice,
                    storageOptions);
        }

        /**
         * Class representing a storage option.
         */
        public static class DbsStorageOption extends GenericDbPriceBundle.GenericStorageOption {
            // This is the possible increment in GB in this option (ex. 250GB).
            final long percentIncrement;

            /**
             * Defines the number of percents we use to calculate new commodity value:
             * new value should be N% greater than previous.
             * E.g. if this number is 10%, and we resize 500GB storage, miniumum new value
             * is 500 + (500 * 10%) = 550GB.
             *
             * @return commodity minimum increase in percentage
             */
            public long getPercentIncrement() {
                return percentIncrement;
            }

            /**
             * Constructor for the storage option.
             *
             * @param increment the increment.
             * @param endRange The end range.
             * @param price The price.
             */
            public DbsStorageOption(long increment, long endRange, double price) {
                super(endRange, price);
                this.percentIncrement = increment;
            }
        }
    }
}
