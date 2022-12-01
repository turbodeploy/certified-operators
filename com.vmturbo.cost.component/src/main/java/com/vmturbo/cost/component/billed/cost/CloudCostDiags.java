package com.vmturbo.cost.component.billed.cost;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;

/**
 * A store for {@link BilledCostData}.
 */
public interface CloudCostDiags {
    /**
     * Set the exportCloudCostDiags feature flag.
     * @param exportCloudCostDiags The feature flag status.
     */
    @Nonnull
    void setExportCloudCostDiags(boolean exportCloudCostDiags);

    /**
     * Gets the exportCloudCostDiags feature flag.
     * @return The feature flag status.
     */
    @Nonnull
    boolean getExportCloudCostDiags();
}