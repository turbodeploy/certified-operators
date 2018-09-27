package com.vmturbo.cost.calculation.integration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
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
    @Immutable
    class CloudCostData {

        private final PriceTable combinedPriceTable;

        private final Map<Long, Discount> discountsByAccount;

        private final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId;

        private final Map<Long, ReservedInstanceData> riBoughtDataById;

        public CloudCostData(@Nonnull final PriceTable priceTable,
                             @Nonnull final Map<Long, Discount> discountsByAccount,
                             @Nonnull final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId,
                             @Nonnull final Map<Long, ReservedInstanceBought> riBoughtById,
                             @Nonnull final Map<Long, ReservedInstanceSpec> riSpecById) {
            this.combinedPriceTable = Objects.requireNonNull(priceTable);
            this.discountsByAccount = Objects.requireNonNull(discountsByAccount);
            this.riCoverageByEntityId = Objects.requireNonNull(riCoverageByEntityId);

            // Combine RI Bought and RI Specs.
            this.riBoughtDataById = riBoughtById.values().stream()
                .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
        }

        @Nonnull
        public PriceTable getPriceTable() {
            return combinedPriceTable;
        }

        @Nonnull
        public Optional<Discount> getDiscountForAccount(final long accountId) {
            return Optional.ofNullable(discountsByAccount.get(accountId));
        }

        @Nonnull
        public Optional<EntityReservedInstanceCoverage> getRiCoverageForEntity(final long entityId) {
            return Optional.ofNullable(riCoverageByEntityId.get(entityId));
        }

        @Nonnull
        public Optional<ReservedInstanceData> getRiBoughtData(final long riBoughtId) {
            return Optional.ofNullable(riBoughtDataById.get(riBoughtId));
        }

    }

    /**
     * A semantically-meaningful tuple of information about an RI purchase.
     */
    @Immutable
    class ReservedInstanceData {
        /**
         * The {@link ReservedInstanceBought} object describing the RI purchase.
         */
        private final ReservedInstanceBought reservedInstanceBought;

        /**
         * The {@link ReservedInstanceSpec} object describing non-purchase-specific details about
         * the RI.
         */
        private final ReservedInstanceSpec reservedInstanceSpec;

        public ReservedInstanceData(@Nonnull final ReservedInstanceBought reservedInstanceBought,
                                    @Nonnull final ReservedInstanceSpec reservedInstanceSpec) {
            this.reservedInstanceBought = reservedInstanceBought;
            this.reservedInstanceSpec = reservedInstanceSpec;
        }

        @Nonnull
        public ReservedInstanceBought getReservedInstanceBought() {
            return reservedInstanceBought;
        }

        @Nonnull
        public ReservedInstanceSpec getReservedInstanceSpec() {
            return reservedInstanceSpec;
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
