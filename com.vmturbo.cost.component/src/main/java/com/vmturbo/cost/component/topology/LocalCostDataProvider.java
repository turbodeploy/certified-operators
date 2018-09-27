package com.vmturbo.cost.component.topology;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.sql.utils.DbException;

/**
 * A {@link CloudCostDataProvider} that gets the data locally from within the cost component.
 */
public class LocalCostDataProvider implements CloudCostDataProvider {

    private final PriceTableStore priceTableStore;

    private final DiscountStore discountStore;

    private final ReservedInstanceBoughtStore riBoughtStore;

    private final ReservedInstanceSpecStore riSpecStore;

    public LocalCostDataProvider(@Nonnull final PriceTableStore priceTableStore,
                                 @Nonnull final DiscountStore discountStore,
                                 @Nonnull final ReservedInstanceBoughtStore riBoughtStore,
                                 @Nonnull final ReservedInstanceSpecStore riSpecStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.discountStore = Objects.requireNonNull(discountStore);
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
    }

    @Nonnull
    @Override
    public CloudCostData getCloudCostData() throws CloudCostDataRetrievalException {
        try {
            final Map<Long, Discount> discountsByAccountId = discountStore.getAllDiscount().stream()
                    .collect(Collectors.toMap(Discount::getAssociatedAccountId, Function.identity()));
            final Map<Long, ReservedInstanceBought> riBoughtById =
                riBoughtStore.getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.newBuilder()
                        .build()).stream()
                    .collect(Collectors.toMap(ReservedInstanceBought::getId, Function.identity()));
            final Map<Long, ReservedInstanceSpec> riSpecById = riSpecStore.getAllReservedInstanceSpec().stream()
                    .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));
            return new CloudCostData(priceTableStore.getMergedPriceTable(),
                    discountsByAccountId,
                    // TODO (roman, Sept 18 2018): Fetch the entity coverage map after Patrick's
                    // change to upload billing data.
                    Collections.emptyMap(),
                    riBoughtById,
                    riSpecById);
        } catch (DbException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}
