package com.vmturbo.cost.calculation.integration;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.AccountPricingData;

/**
 *  * An interface used for resolving Business Account to AccountPricingData mapping.
 *
 * @param <ENTITY_TYPE> The entity type the pricing applies to.
 */
public interface PricingResolver<ENTITY_TYPE> {

    /**
     * Returns the mapping of business account to the correct account pricing data object.
     *
     * @param cloudTopology The cloud topology.
     *
     * @return The mapping of Business Account oid to Price Table Mapping.
     * @throws CloudCostDataRetrievalException A cloud cost data retrieval exception.
     */
    Map<Long, AccountPricingData<TopologyEntityDTO>> getAccountPricingDataByBusinessAccount(
            @Nonnull CloudTopology<ENTITY_TYPE> cloudTopology)
            throws CloudCostDataRetrievalException;

    /**
     * Loads and returns a map of business account to the associates {@link ReservedInstancePriceTable}.
     * @param cloudTopology The cloud topology, used to load the set of accounts to query. If the topology
     *                      contains no business accounts, all mappings will be returned.
     * @return An immutable map of account OID to {@link ReservedInstancePriceTable} mappings.
     */
    @Nonnull
    Map<Long, ReservedInstancePriceTable> getRIPriceTablesByAccount(
            @Nonnull CloudTopology<ENTITY_TYPE> cloudTopology);
}

