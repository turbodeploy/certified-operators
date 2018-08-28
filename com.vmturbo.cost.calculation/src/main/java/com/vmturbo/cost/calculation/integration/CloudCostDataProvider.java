package com.vmturbo.cost.calculation.integration;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;

/**
 * An interface provided by the users of the cost calculation library to get the
 * {@link CloudCostData} required for cost calculation from wherever the cost data is stored.
 *
 * For example, at the time of this writing the cost component will need to get the cost data
 * from an internal store, whereas the market component will need to get this data from the
 * cost component.
 */
public interface CloudCostDataProvider {

    /**
     * Get the cloud cost data from this particular provider. The cloud cost data is retrieved
     * in bulk via a single call.
     *
     * @return The {@link CloudCostData}.
     * @throws CloudCostDataRetrievalException If there is an error retrieving the data.
     */
    @Nonnull
    CloudCostData getCloudCostData() throws CloudCostDataRetrievalException;

    /**
     * The bundle of non-topology data required to compute costs. This can include things like
     * the {@link PriceTable} from the cost probes, the discounts for various business accounts,
     * and the reserved instance coverage.
     */
    class CloudCostData {

        private final PriceTable combinedPriceTable;

        public CloudCostData(@Nonnull final PriceTable priceTable) {
            this.combinedPriceTable = Objects.requireNonNull(priceTable);
        }

        @Nonnull
        public PriceTable getPriceTable() {
            return combinedPriceTable;
        }
    }

    /**
     * Wrapper class for exceptions encountered when retrieving {@link CloudCostData}.
     */
    class CloudCostDataRetrievalException extends Exception {

        public CloudCostDataRetrievalException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}
