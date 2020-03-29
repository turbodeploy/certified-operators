package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Class to convert EntityCost from DB to StatRecord ready for RPC service consumption.
 */
public class EntityCostToStatRecordConverter {

    /**
     * Private constructor.
     */
    private EntityCostToStatRecordConverter(){}

    /**
     * Static method to convert entityCost to Collection of StatRecords.
     *
     * @param entityCost given EntityCost.
     * @return Collection of StatRecord based on CostComponent in the cost entity.
     * return Empty List if null.
     */
    public static Collection<StatRecord> convertEntityToStatRecord(@Nullable final EntityCost entityCost) {
        if (entityCost == null) {
            return Collections.emptyList();
        }
        final Collection<StatRecord> statRecords = Lists.newArrayList();
        entityCost.getComponentCostList().forEach(componentCost -> {
            final StatRecord.Builder builder = StatRecord.newBuilder();
            final float amount = (float)componentCost.getAmount().getAmount();
            builder.setAssociatedEntityId(entityCost.getAssociatedEntityId());
            builder.setAssociatedEntityType(entityCost.getAssociatedEntityType());
            builder.setCategory(componentCost.getCategory());
            builder.setName(StringConstants.COST_PRICE);
            builder.setUnits("$/h");
            builder.setValues(StatValue.newBuilder()
                    .setAvg(amount)
                    .setMax(amount)
                    .setMin(amount)
                    .setTotal(amount)
                    .build());
            statRecords.add(builder.build());
        });
        return statRecords;
    }

    /**
     * Wrapper method for {@link #convertEntityToStatRecord}.
     *
     * @param entityCosts list of Entities.
     * @return flattened StatRecords.
     */
    public static Collection<StatRecord> convertEntityToStatRecord(@Nonnull final Collection<EntityCost> entityCosts) {
        if (entityCosts.isEmpty()) {
            return Collections.emptyList();
        }
        final List<StatRecord> statRecords = new ArrayList<>();
        entityCosts.forEach(entityCost -> {
            statRecords.addAll(convertEntityToStatRecord(entityCost));
        });
        return statRecords;
    }

}
