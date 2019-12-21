package com.vmturbo.market.cloudscaling.sma.entities;

/**
 * Stable Mariage algorithm enumerated type for Platform.
 */
public enum SMAPlatform {
    /**
     * unknown os.
     */
    UNKNOWNN,
    /**
     * linux os. if regional will be isf.
     */
    LINUX,
    /**
     * redhat os.
     */
    REDHAT,
    /**
     * windows os. can never be isf.
     */
    WINDOWS,
    /**
     * linux with sql enterprise.
     */
    LINUX_WITH_SQL_ENTERPRISE,
    /**
     * linux with sql standard.
     */
    LINUX_WITH_SQL_STANDARD,
    /**
     * linux with sql web.
     */
    LINUX_WITH_SQL_WEB,
    /**
     * windows with sql standard.
     */
    WINDOWS_WITH_SQL_STANDARD,
    /**
     * windows with sql web.
     */
    WINDOWS_WITH_SQL_WEB,
    /**
     * windows with sql enterprise.
     */
    WINDOWS_WITH_SQL_ENTERPRISE,
    /**
     * windows with bring your own license.
     */
    WINDOWS_BYOL,
    /**
     * windows server.
     */
    WINDOWS_SERVER,
    /**
     * windows server burst.
     */
    WINDOWS_SERVER_BURST;
}