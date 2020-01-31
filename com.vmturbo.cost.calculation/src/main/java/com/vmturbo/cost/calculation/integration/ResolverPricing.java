package com.vmturbo.cost.calculation.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.PricingDataIdentifier;

/**
 * Abstract class for resolving pricing of business accounts to account pricing data.
 */
public abstract class ResolverPricing implements PricingResolver {

    /**
     * A map of the PricingDataIdentifier to the Account Pricing data.
     */
    protected ConcurrentHashMap<PricingDataIdentifier, AccountPricingData<TopologyEntityDTO>>
            accountPricingDataMapByPricingDataIdentifier = new ConcurrentHashMap<>();

    /**
     * Given a map of price table key oid by business account oid and priceTable by priceTableKey oid,
     * return a map of price table by business account oid.
     *
     * @param priceTableKeyOidByBusinessAccountOid The priceTableKeyOidByBusinessAccountOid map.
     * @param priceTableByPriceTableKeyOid The priceTableByPriceTableKeyOid map.
     *
     * @return The Price Table by business account mapping.
     */
    protected Map<Long, PriceTable> getPriceTableByBusinessAccountOid(Map<Long, Long> priceTableKeyOidByBusinessAccountOid,
                                                                    Map<Long, PriceTable> priceTableByPriceTableKeyOid) {
        Map<Long, PriceTable> accountPriceTableByBusinessAccountOid = new HashMap<>();
        for (Map.Entry<Long, Long> entry: priceTableKeyOidByBusinessAccountOid.entrySet()) {
            Long baOid = entry.getKey();
            Long priceTableKeyOid = entry.getValue();
            if (priceTableByPriceTableKeyOid.get(priceTableKeyOid) != null) {
                PriceTable priceTable = priceTableByPriceTableKeyOid.get(priceTableKeyOid);
                accountPriceTableByBusinessAccountOid.put(baOid, priceTable);
            }
        }
        return accountPriceTableByBusinessAccountOid;
    }
}
