package com.vmturbo.cost.component.topology;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.sql.utils.DbException;

/**
 * A {@link CloudCostDataProvider} that gets the data locally from within the cost component.
 */
public class LocalCostDataProvider implements CloudCostDataProvider {

    private final PriceTableStore priceTableStore;

    private final DiscountStore discountStore;

    public LocalCostDataProvider(@Nonnull final PriceTableStore priceTableStore,
                                 @Nonnull final DiscountStore discountStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.discountStore = Objects.requireNonNull(discountStore);
    }

    @Nonnull
    @Override
    public CloudCostData getCloudCostData() throws CloudCostDataRetrievalException {
        try {
            final Map<Long, Discount> discountsByAccountId = discountStore.getAllDiscount().stream()
                    .collect(Collectors.toMap(Discount::getAssociatedAccountId, Function.identity()));
            return new CloudCostData(priceTableStore.getMergedPriceTable(), discountsByAccountId);
        } catch (DbException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}
