package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher.ReservedInstanceSpecMatcherFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The spec matcher is used to match recorded demand to recommendation specs (those specs that
 * meet the purchase profile of the analysis).
 *
 * @param <SPEC_TYPE> The spec type.
 */
public interface CloudCommitmentSpecMatcher<SPEC_TYPE> {

    /**
     * Matches demand to cca specs filtered on purchase constraints.
     *
     * @param cloudTierDemand The cloud tier demand.
     * @param <T> The type of the commitment spec data.
     * @return Matching ReservedInstanceSpecData if found.
     */
    <T extends CloudCommitmentSpecData<SPEC_TYPE>> Optional<T> matchDemandToSpecs(
            ScopedCloudTierInfo cloudTierDemand);

    /**
     * A factory interface for creating instances of {@link CloudCommitmentSpecMatcher}.
     */
    interface CloudCommitmentSpecMatcherFactory {

        /**
         * Creates a new {@link CloudCommitmentSpecMatcher} instance.
         * @param cloudTopology The cloud topology, containing cloud tiers referenced by the specs. The
         *                      cloud topology is used to determine relationships between multiple cloud
         *                      tiers.
         * @param purchaseProfile The purchase profile, used to filter the RI spec inventory to
         *                        only those RI specs applicable to a recommendation.
         * @return The newly created instance of {@link CloudCommitmentSpecMatcher}.
         */
        @Nonnull
        CloudCommitmentSpecMatcher<?> createSpecMatcher(
                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                CommitmentPurchaseProfile purchaseProfile);
    }

    /**
     * {@inheritDoc}.
     */
    class DefaultCloudCommitmentSpecMatcherFactory implements CloudCommitmentSpecMatcherFactory {

        private final ReservedInstanceSpecMatcherFactory reservedInstanceSpecMatcherFactory;

        private final RISpecPurchaseFilterFactory riSpecPurchaseFilterFactory;

        /**
         * Constructs a new {@link CloudCommitmentSpecMatcherFactory} instance.
         * @param reservedInstanceSpecMatcherFactory A factory for creating {@link ReservedInstanceSpecMatcher}
         *                                           instances, which will be created if the purchase
         *                                           profile is requesting RI recommendations.
         * @param riSpecPurchaseFilterFactory A factory for creating {@link RISpecPurchaseFilter}
         *                                    instances, which filter the RI spec inventory based
         *                                    on the purchase profile constraints.
         */
        public DefaultCloudCommitmentSpecMatcherFactory(
                @Nonnull ReservedInstanceSpecMatcherFactory reservedInstanceSpecMatcherFactory,
                @Nonnull RISpecPurchaseFilterFactory riSpecPurchaseFilterFactory) {

            this.reservedInstanceSpecMatcherFactory = Objects.requireNonNull(reservedInstanceSpecMatcherFactory);
            this.riSpecPurchaseFilterFactory = Objects.requireNonNull(riSpecPurchaseFilterFactory);
        }

        /**
         * {@inheritDoc}.
         */
        @Nonnull
        @Override
        public CloudCommitmentSpecMatcher<?> createSpecMatcher(
                @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
                final CommitmentPurchaseProfile purchaseProfile) {

            if (purchaseProfile.hasRiPurchaseProfile()) {
                final RISpecPurchaseFilter riSpecPurchaseFilter = riSpecPurchaseFilterFactory.createFilter(
                        cloudTopology,
                        purchaseProfile);

                return reservedInstanceSpecMatcherFactory.newMatcher(riSpecPurchaseFilter);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }
}
