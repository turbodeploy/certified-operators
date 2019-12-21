package com.vmturbo.market.cloudvmscaling.analysis;

import java.util.EnumSet;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;

/**
 * Contains constant definitions for Stable Marriage Algorithm (SMA).
 */
public class SMAUtils {
    /**
     * oid for regional.
     */
    public static final long NO_ZONE = -1;
    /**
     * oid for vm's that does not belong to any ASG.
     */
    public static final long NO_GROUP_OID = -1;
    /**
     * epsilon for floating point comparison.
     */
    public static final double EPSILON = 1e-20;
    /**
     * ROUNDING to trim floating point for comparison.
     */
    public static final float ROUNDING = 100000f;
    /**
     * Set of Linux OS. Used to determine if the RI is ISF.
     */
    public static final EnumSet<SMAPlatform> LINUX_OS =
            EnumSet.of(SMAPlatform.LINUX,
                    SMAPlatform.LINUX_WITH_SQL_ENTERPRISE,
                    SMAPlatform.LINUX_WITH_SQL_STANDARD,
                    SMAPlatform.LINUX_WITH_SQL_WEB);
}
