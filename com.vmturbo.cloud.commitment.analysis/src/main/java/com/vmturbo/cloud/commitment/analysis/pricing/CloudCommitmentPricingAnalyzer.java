package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * Interface representing the CloudCommitmentPricinganalyzer. This interface is responsible for resolving the pricing
 * of.
 */
public interface CloudCommitmentPricingAnalyzer {

    /**
     * Returns the tier demand pricing data for demand scoped to a tier, account and OS.
     *
     * @param cloudTierDemand The cloud tier demand we wish to get pricing for.
     *
     * @return The tier demand pricing data.
     */
    CloudTierPricingData getTierDemandPricing(ScopedCloudTierInfo cloudTierDemand);

    /**
     * This method resolves the CCA related pricing. For example, resolves the RI recurring and upfront cost.
     * The method returns the cheapest pricing for the spec scoped to by the different accounts belonging
     * to demand.
     *
     * @param cloudCommitmentSpecData The spec data to resolve pricing for.
     * @param potentialPurchasingAccountOids The set of business account ids.
     *
     * @return The cloud commitment pricing data.
     */
    CloudCommitmentPricingData getCloudCommitmentPricing(
            CloudCommitmentSpecData cloudCommitmentSpecData,
            Set<Long> potentialPurchasingAccountOids);

    /**
     * A factory class for creating {@link CloudCommitmentPricingAnalyzer} instances.
     */
    interface CloudCommitmentPricingAnalyzerFactory {

        /**
         * Constructs a new pricing analyzer.
         * @param cloudTierTopology A topology containing the cloud tiers relevant to any queried
         *                          pricing information.
         * @return The newly constructed pricing analyzer.
         * @throws CloudCostDataRetrievalException An exception indicating the cloud cost data could
         * not be successfully retrieved.
         */
        @Nonnull
        CloudCommitmentPricingAnalyzer newPricingAnalyzer(
                @Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology) throws CloudCostDataRetrievalException;
    }
}
