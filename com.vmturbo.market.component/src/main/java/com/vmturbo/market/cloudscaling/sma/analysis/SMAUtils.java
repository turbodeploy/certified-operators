package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.EnumSet;

import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Contains constant definitions for Stable Marriage Algorithm (SMA).
 */
public class SMAUtils {
    /**
     * OID for zonal ID, when there is no zone.
     */
    public static final long NO_ZONE = -1;
    /**
     * oid for vm's that does not belong to any ASG.
     */
    public static final String NO_GROUP_ID = new String();
    /**
     * epsilon for floating point comparison.
     */
    public static final double EPSILON = 1e-20;
    /**
     * Improvement factor for a vm to be discounted by an RI.
     */
    public static final double IMPROVEMENT_FACTOR = 0.01f;
    /**
     * ROUNDING to trim floating point for comparison.
     */
    public static final float ROUNDING = 100000f;
    /**
     * maximum iterations to run for the SMA to converge.
     */
    public static final int MAX_ITERATIONS = 5;

    /**
     * Round the float to a fixed number of decimal places determined by ROUNDING.
     * @param input the float to be rounded.
     * @return float rounded to a fixed number of decimals.
     */
    public static float round(float input) {
        return (float)Math.round(input *
                SMAUtils.ROUNDING) / SMAUtils.ROUNDING;
    }

    /**
     * Set of Linux OS. Used to determine if the RI is ISF.
     */
    public static final EnumSet<OSType> LINUX_OS =
        EnumSet.of(OSType.LINUX,
            OSType.LINUX_WITH_SQL_ENTERPRISE,
            OSType.LINUX_WITH_SQL_STANDARD,
            OSType.LINUX_WITH_SQL_WEB);

    /**
     * When there is no RI coverage.
     */
    public static final float NO_RI_COVERAGE = 0;

    /**
     * When there is no RI coverage.
     */
    public static final long NO_CURRENT_RI = -1;

    /**
     * zero cost.
     */
    public static final SMACost zeroCost = new SMACost(0.0f, 0.0f);

    /**
     * Placeholder for invalid SMAContext.
     */
    public static final SMAContext BOGUS_CONTEXT = new SMAContext(SMACSP.UNKNOWN, OSType.UNKNOWN_OS,
        -1, -1, Tenancy.DEDICATED);

    /**
     * Placeholder for invalid SMATemplate.
     */
    public static final SMATemplate BOGUS_TEMPLATE = new SMATemplate(-1, "SMATemplate placeholder",
        "xxx", 0, BOGUS_CONTEXT, null);

}
