package com.vmturbo.cost.component.billedcosts;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.platform.sdk.common.CostBilling;

/**
 * A type that contains data operations for a Billed Cost table.
 */
public interface BilledCostStore {

    /**
     * Inserts billing data points into the Billed Cost table.
     *
     * @param points to be inserted into Billed Cost table.
     * @param discoveredTagGroupIdToOid map to lookup tag group id of the points.
     * @param granularity (hourly, daily or monthly) of the data points being inserted.
     * @return list of futures for submitted insert tasks.
     */
    List<Future<Integer>> insertBillingDataPoints(
        @Nonnull List<Cost.UploadBilledCostRequest.BillingDataPoint> points,
        @Nonnull Map<Long, Long> discoveredTagGroupIdToOid,
        @Nonnull CostBilling.CloudBillingData.CloudBillingBucket.Granularity granularity);
}