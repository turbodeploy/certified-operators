package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Map;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Encapsulates the output of a calculation of uncovered demand as the application of RI inventory
 * to tracked demand.
 */
@Value.Immutable
public interface RIBuyDemandCalculationInfo {

    /**
     * The calculated uncovered demand by compute tier. The value represents the recorded demand
     * in coupons.
     * @return An immutable map of compute tier to recorded demand in coupons.
     */
    Map<TopologyEntityDTO, float[]> uncoveredDemandByComputeTier();

    /**
     * The aggregate uncovered demand within a specific context. The value is normalized to the
     * coupon value of the RI spec to purchase i.e. it is the number of RI spec instances recorded
     * (not coupons).
     *
     * @return An array of the recorded demand normalized to the RI spec to purchase.
     */
    float[] aggregateUncoveredDemand();

    /**
     * Represents the business account with the most recorded uncovered demand in coupons. This account
     * is expected to be used as the purchasing account of any subsequent buy recommendations.
     *
     * @return The OID of the primary account.
     */
    long primaryAccountOid();

    /**
     * Represents the number of hours (out of the recorded week of demand), in which there was a recorded
     * value for this calculation. A recorded value represents a value greater than or equal to 0, meaning
     * active is treated as any recorded value (as long as the demand writer has been able to record
     * a value).
     *
     * @return The number of hours in which demand has been recorded
     */
    int activeHours();

}
