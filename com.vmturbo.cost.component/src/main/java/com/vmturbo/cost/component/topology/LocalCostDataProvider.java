package com.vmturbo.cost.component.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.component.pricing.PriceTableStore;

/**
 * A {@link CloudCostDataProvider} that gets the data locally from within the cost component.
 */
public class LocalCostDataProvider implements CloudCostDataProvider {

    private final PriceTableStore priceTableStore;

    public LocalCostDataProvider(@Nonnull final PriceTableStore priceTableStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
    }

    @Nonnull
    @Override
    public CloudCostData getCloudCostData() throws CloudCostDataRetrievalException {
        return new CloudCostData(priceTableStore.getMergedPriceTable());
    }
}
