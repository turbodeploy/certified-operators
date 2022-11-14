package com.vmturbo.mediation.azure.pricing.util;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.cost.common.CostUtils;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * Interface to provide a price UnitConverter.
 */
public interface UnitConverter {

    /**
     * Price unit conversion map.
     * Those are the options that I found by parsing the price json file:
     * "unit" : "Count",/Target.java
     * "unit" : "GB",
     * "unit" : "GB-Mo",
     * "unit" : "Gbps-hrs",
     * "unit" : "Hrs",
     * "unit" : "IOPS-Mo",
     * "unit" : "MBps-Mo",
     * "unit" : "IOs",
     * "unit" : "IdleLCU-Hr",
     * "unit" : "LCU-Hrs",
     * "unit" : "Quantity",
     * "unit" : "vCPU-Hours",
     */
    Map<String, Unit> priceUnitMap = ImmutableMap.<String, Unit>builder()
            .put("Hrs", Unit.HOURS)
            .put("1/Month", Unit.MONTH)
            .put("GB-Mo", Unit.GB_MONTH) // Used by AWS gp3 tier, etc.
            .put("GB-month", Unit.GB_MONTH) // Used by IO2 tier
            .put("1 GB/Month", Unit.GB_MONTH)
            .put("IOs", Unit.MILLION_IOPS)

            // Used for AWS io1, gp3 and Azure Ultra volumes, matching with the actual IOPS of the volume
            // when calculating the actual per month IOPS cost of the volume
            .put("IOPS-Mo", Unit.MILLION_IOPS)

            // Used for AWS gp3 tier
            .put("MiBps-Mo", Unit.MBPS_MONTH)
            // Used for Ultra volumes, matching with the actual throughput of the volume
            // when calculating the actual per month throughput cost of the volume
            .put("MBps-Mo", Unit.MBPS_MONTH)

            .put("Days", Unit.DAYS)
            .put("Count", Unit.TOTAL) // usually used for: 1st 100 Elastic IP remap are free
            .put("Quantity", Unit.TOTAL) // usually used for fixed one-time cost for RI
            // this is here because in the probe's RI parsing, they are converting this string also,
            // instead of keeping the same string found in the cloud provider file
            .put(CostUtils.COST_UNIT_TOTAL, Unit.TOTAL)
            .build();
}
