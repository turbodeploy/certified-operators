package com.vmturbo.cost.calculation.pricing;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle.DatabasePrice;
import com.vmturbo.cost.calculation.pricing.DatabasePriceBundle.DatabasePrice.StorageOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

/**
 * A bundle of of possible prices for a (database tier, region) combination. The possible
 * prices are affected by the {@link DatabaseEngine} and owning business account of the VM consuming
 * from the tier.
 */
public class DatabasePriceBundle extends GenericDbPriceBundle<DatabasePrice> {

    private DatabasePriceBundle(@Nonnull final List<DatabasePrice> prices) {
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
        private final ImmutableList.Builder<DatabasePrice> priceBuilder = ImmutableList.builder();

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
                @Nonnull final List<StorageOption> storageOptions) {
            priceBuilder.add(
                    new DatabasePrice(accountId, dbEngine, dbEdition, depType, licenseModel,
                            hourlyPrice, storageOptions));
            return this;
        }

        /**
         * Build the db price bundle.
         *
         * @return The Database price bundle.
         */
        @Nonnull
        public DatabasePriceBundle build() {
            return new DatabasePriceBundle(priceBuilder.build());
        }
    }

    /**
     * Temporary object for integration with CostTuples.
     */
    public static class DatabasePrice
            extends GenericDbPriceBundle.GenericPrice<DatabasePrice.StorageOption> {

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
        public DatabasePrice(final long accountId, @Nonnull final DatabaseEngine dbEngine,
                @Nonnull final DatabaseEdition dbEdition, @Nullable final DeploymentType depType,
                @Nullable final LicenseModel licenseModel, final double hourlyPrice,
                @Nonnull final List<StorageOption> storageOptions) {
            super(accountId, dbEngine, dbEdition, depType, licenseModel, hourlyPrice,
                    storageOptions);
        }

        /**
         * Class representing a storage option.
         */
        public static class StorageOption extends GenericDbPriceBundle.GenericStorageOption {
            final long increment;

            /**
             * Possible increment in GB in this option (ex. 250GB).
             *
             * @return possible increment in GB in this option
             */
            public long getIncrement() {
                return increment;
            }

            /**
             * Constructor for the storage option.
             *
             * @param increment the increment.
             * @param endRange The end range.
             * @param price The price.
             */
            public StorageOption(long increment, long endRange, double price) {
                super(endRange, price);
                this.increment = increment;
            }
        }
    }
}
