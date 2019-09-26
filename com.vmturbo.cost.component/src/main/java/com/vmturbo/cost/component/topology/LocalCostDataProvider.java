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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.sql.utils.DbException;

/**
 * A {@link CloudCostDataProvider} that gets the data locally from within the cost component.
 */
public class LocalCostDataProvider implements CloudCostDataProvider {

    private final PriceTableStore priceTableStore;

    private final DiscountStore discountStore;

    private final ReservedInstanceBoughtStore riBoughtStore;

    private final ReservedInstanceSpecStore riSpecStore;

    private final EntityReservedInstanceMappingStore entityRiMappingStore;

    public LocalCostDataProvider(@Nonnull final PriceTableStore priceTableStore,
                 @Nonnull final DiscountStore discountStore,
                 @Nonnull final ReservedInstanceBoughtStore riBoughtStore,
                 @Nonnull final ReservedInstanceSpecStore riSpecStore,
                 @Nonnull final EntityReservedInstanceMappingStore entityRiMappingStore) {
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.discountStore = Objects.requireNonNull(discountStore);
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
        this.riSpecStore = Objects.requireNonNull(riSpecStore);
        this.entityRiMappingStore = Objects.requireNonNull(entityRiMappingStore);
    }

    @Nonnull
    @Override
    public CloudCostData getCloudCostData(TopologyInfo topoInfo) throws CloudCostDataRetrievalException {
        try {
            final Map<Long, Discount> discountsByAccountId = discountStore.getAllDiscount().stream()
                    .collect(Collectors.toMap(Discount::getAssociatedAccountId, Function.identity()));
            final Map<Long, ReservedInstanceBought> riBoughtById =
                riBoughtStore.getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.newBuilder()
                        .addAllScopeId(topoInfo.getScopeSeedOidsList())
                        .setScopeEntityType(topoInfo.getScopeEntityType())
                        .build()).stream()
                    .collect(Collectors.toMap(ReservedInstanceBought::getId, Function.identity()));
            final Map<Long, ReservedInstanceSpec> riSpecById = riSpecStore.getAllReservedInstanceSpec().stream()
                    .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));
            return new CloudCostData(priceTableStore.getMergedPriceTable(),
                    discountsByAccountId,
                    entityRiMappingStore.getEntityRiCoverage(),
                    riBoughtById,
                    riSpecById,
                    Collections.emptyMap());
        } catch (DbException e) {
            throw new CloudCostDataRetrievalException(e);
        }
    }
}
