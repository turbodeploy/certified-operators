package com.vmturbo.cost.calculation.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

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
     * @param topologyInfo
     * @return The {@link CloudCostData}.
     * @throws CloudCostDataRetrievalException If there is an error retrieving the data.
     */
    @Nonnull
    CloudCostData getCloudCostData(@Nonnull TopologyInfo topoInfo) throws CloudCostDataRetrievalException;

    /**
     * The bundle of non-topology data required to compute costs. This can include things like
     * the {@link PriceTable} from the cost probes, the discounts for various business accounts,
     * and the reserved instance coverage.
     */
    @Immutable
    class CloudCostData {

        private static final CloudCostData EMPTY = new CloudCostData(PriceTable.getDefaultInstance(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        private final PriceTable combinedPriceTable;

        private final Map<Long, Discount> discountsByAccount;

        private final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId;

        private final Map<Long, ReservedInstanceData> riBoughtDataById;

        private final Map<Long, ReservedInstanceData> buyRIBoughtDataById;

        public CloudCostData(@Nonnull final PriceTable priceTable,
                             @Nonnull final Map<Long, Discount> discountsByAccount,
                             @Nonnull final Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityId,
                             @Nonnull final Map<Long, ReservedInstanceBought> riBoughtById,
                             @Nonnull final Map<Long, ReservedInstanceSpec> riSpecById,
                             @Nonnull final Map<Long, ReservedInstanceBought> buyRIBoughtById) {
            this.combinedPriceTable = Objects.requireNonNull(priceTable);
            this.discountsByAccount = Objects.requireNonNull(discountsByAccount);
            this.riCoverageByEntityId = Objects.requireNonNull(riCoverageByEntityId);

            // Combine RI Bought and RI Specs.
            this.riBoughtDataById = riBoughtById.values().stream()
                .filter(riBought -> riSpecById.containsKey(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec()))
                .map(riBought -> new ReservedInstanceData(riBought, riSpecById.get(riBought.getReservedInstanceBoughtInfo().getReservedInstanceSpec())))
                .collect(Collectors.toMap(riData -> riData.getReservedInstanceBought().getId(), Function.identity()));
            this.buyRIBoughtDataById = buyRIBoughtById.values().stream()
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
        public Map<Long, EntityReservedInstanceCoverage> getCurrentRiCoverage() {
            return riCoverageByEntityId;
        }

        @Nonnull
        public Optional<ReservedInstanceData> getExistingRiBoughtData(final long riBoughtId) {
            return Optional.ofNullable(riBoughtDataById.get(riBoughtId));
        }

        /**
         * This will return a read only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory. This will be used in real time analysis.
         *
         * @return Collection of {@link ReservedInstanceData} representing the existing RIs.
         */
        @Nonnull
        public Collection<ReservedInstanceData> getExistingRiBought() {
            return Collections.unmodifiableCollection(riBoughtDataById.values());
        }

        /**
         * This will return a read only collection of {@link ReservedInstanceData} representing
         * all the existing RIs in the inventory and the Buy RI recommendations. This will be used
         * during Optimize Cloud Plans.
         *
         * @return Read only Collection of {@link ReservedInstanceData} representing the existing
         * RIs bought and the Buy RI recommendations
         */
        @Nonnull
        public Collection<ReservedInstanceData> getAllRiBought() {
            List<ReservedInstanceData> allRiData = new ArrayList<>();
            allRiData.addAll(riBoughtDataById.values());
            allRiData.addAll(buyRIBoughtDataById.values());
            return Collections.unmodifiableCollection(allRiData);
        }

        /**
         * Utility method to create an empty {@link CloudCostData}. Useful in testing, mocking,
         * or to continue operations after {@link CloudCostDataProvider#getCloudCostData()} throws
         * an exception.
         *
         * @return An empty {@link CloudCostData}.
         */
        @Nonnull
        public static CloudCostData empty() {
            return EMPTY;
        }

    }

    /**
     * A semantically-meaningful tuple of information about an RI purchase.
     */
    @Immutable
    public class ReservedInstanceData {
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

        /**
         * Check if riData is valid.
         * There are 2 situations when riData might be invalid:
         * 1. when cost probe is sending the data even though there is no cloud target
         * 2. when you have cloud target but in the initial rounds the reserved instances
         *    are discovered before the topology entities
         * 3. when in a cloud plan, the topology is scoped and not all reserved instances
         * are in that scope.
         * @param topology the topology
         * @return true, if tierId_ of riSpec of riData is in the topology. false, if not.
         */
        public boolean isValid(@Nonnull Map<Long, TopologyEntityDTO> topology) {
            return topology.get(reservedInstanceSpec.getReservedInstanceSpecInfo().getTierId())
                    // checking region id to exclude Ri that is not within the scoped region
                    != null && topology.get(new Long(reservedInstanceSpec
                            .getReservedInstanceSpecInfo().getRegionId())) != null;
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
