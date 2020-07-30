package com.vmturbo.market.cloudscaling.sma.analysis;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Contains constant definitions for Stable Marriage Algorithm (SMA).
 */
public class SMAUtils {

    /**
     * When the name is UNKNOWN.
     */
    public static final String UNKNOWN_NAME = "-";
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
    public static final long UNKNOWN_OID = -1;

    /**
     * no cost.
     */
    public static final float NO_COST = 0f;

    /**
     * zero SMACost.
     */
    public static final SMACost zeroSMACost = new SMACost(NO_COST, NO_COST);

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

    /**
     * if Azure, then OS type agnostic and use UNKNOWN_OS.
     */
    public static final Set<OSType> UNKNOWN_OS_SINGLETON_SET = Collections.singleton(OSType.UNKNOWN_OS);

    /**
     * Placeholder for an invalid RI.
     */
    public static final SMAReservedInstance BOGUS_RI = new SMAReservedInstance(UNKNOWN_OID, UNKNOWN_OID,
        UNKNOWN_NAME, UNKNOWN_OID, BOGUS_TEMPLATE, UNKNOWN_OID, 0, false, false, false);

    /**
     * Ensures there are only 4 significant decimal places.  Round up if needed.
     * @param value float value
     * @return float value with at most 4 significant decimal places.
     */
    public static float format4Digits(float value) {
        return ((int)((value + 0.00005) * 10000)) / (float)10000.0f;
    }

}
