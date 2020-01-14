package com.vmturbo.cost.calculation.integration;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.AccountPricingData;

/**
 * An interface used for resolving Business Account to AccountPricingData mapping.
 */
public interface PricingResolver {

    /**
     * Returns the mapping of business account to the correct account pricing data object.
     *
     * @param cloudTopo The cloud topology.
     *
     * @return The mapping of Business Account oid to Price Table Mapping.
     * @throws CloudCostDataRetrievalException A cloud cost data retrieval exception.
     */
    Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopo)
            throws CloudCostDataRetrievalException;
}

