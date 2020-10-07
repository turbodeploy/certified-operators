package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Set;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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
     * @param cloudTopology The cloud tier topology used for constructing pricing info and getting the tiers.
     *
     * @return The tier demand pricing data.
     */
    TierDemandPricingData getTierDemandPricing(ScopedCloudTierDemand cloudTierDemand, CloudTopology<TopologyEntityDTO> cloudTopology);

    /**
     * This method resolves the CCA related pricing. For example, resolves the RI recurring and upfront cost.
     * The method returns the cheapest pricing for the spec scoped to by the different accounts belonging
     * to demand.
     *
     * @param cloudCommitmentSpecData The spec data to resolve pricing for.
     * @param businessAccountOids The set of business account ids.
     * @param cloudTopology The cloud topology used for constructing pricing info and getting the tiers.
     *
     * @return The cloud commitment pricing data.
     */
    CloudCommitmentPricingData getCloudCommitmentPricing(CloudCommitmentSpecData cloudCommitmentSpecData,
            Set<Long> businessAccountOids, CloudTopology<TopologyEntityDTO> cloudTopology);
}
