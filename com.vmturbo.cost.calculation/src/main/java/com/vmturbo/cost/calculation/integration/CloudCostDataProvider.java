package com.vmturbo.cost.calculation.integration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
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
    public class CloudCostData {

        private final PriceTable combinedPriceTable;

        private final Map<Long, Discount> discountsByAccount;

        public CloudCostData(@Nonnull final PriceTable priceTable,
                             @Nonnull final Map<Long, Discount> discountsByAccount) {
            this.combinedPriceTable = Objects.requireNonNull(priceTable);
            this.discountsByAccount = Objects.requireNonNull(discountsByAccount);
        }

        @Nonnull
        public PriceTable getPriceTable() {
            return combinedPriceTable;
        }

        @Nonnull
        public Optional<Discount> getDiscountForAccount(final long accountId) {
            return Optional.ofNullable(discountsByAccount.get(accountId));
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
