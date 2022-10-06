package com.vmturbo.cost.component.billed.cost;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.persistence.DataBatcher.DirectDataBatcher;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;

/**
 * Responsible for breaking up {@link BilledCostData} instances into batches nad determining the size of a
 * complete batch. Currently, treats all {@link BilledCostData} instances as a complete batch.
 */
public class BilledCostBatcher extends DirectDataBatcher<BilledCostData> {

    private BilledCostBatcher() {}

    /**
     * Creates a new {@link BilledCostBatcher} instance.
     * @return The new {@link BilledCostBatcher} instance.
     */
    @Nonnull
    public static BilledCostBatcher create() {
        return new BilledCostBatcher();
    }
}
