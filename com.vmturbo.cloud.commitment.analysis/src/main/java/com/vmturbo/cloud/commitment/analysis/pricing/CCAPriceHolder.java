package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;

/**
 * Utility class required for holding price tables for resolving CCA pricing.
 */
public class CCAPriceHolder {

    private final Map<Long, AccountPricingData> accountPricingDataMap;

    private final Map<Long, ReservedInstancePriceTable> riPriceTableMap;

    private final CloudRateExtractor cloudRateExtractor;

    /**
     * Constructor the CCA Price Holder.
     *
     * @param accountPricingDataMap Account Pricing Data having reference to the price tables and discount applicators
     * per account.
     * @param riPriceTableMap The RI Price Table mapped by account.
     * @param cloudRateExtractor The price table class which encapsulated methods to retrieve and query pricing.
     */
    public CCAPriceHolder(@Nonnull Map<Long, AccountPricingData> accountPricingDataMap,
            @Nonnull Map<Long, ReservedInstancePriceTable> riPriceTableMap, CloudRateExtractor cloudRateExtractor) {
        this.accountPricingDataMap = accountPricingDataMap;
        this.riPriceTableMap = riPriceTableMap;
        this.cloudRateExtractor = cloudRateExtractor;
    }

    public Map<Long, AccountPricingData> getAccountPricingDataMap() {
        return accountPricingDataMap;
    }

    public Map<Long, ReservedInstancePriceTable> getRiPriceTableMap() {
        return riPriceTableMap;
    }

    public CloudRateExtractor getCloudRateExtractor() {
        return cloudRateExtractor;
    }
}
