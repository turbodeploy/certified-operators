package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Testing jitter. Small changes in environment results in small changes in output
 */

public class Jitter {
    /**
     * run the jitter test for ISF, regional windows and zonal windows.
     */
    @Ignore
    @Test
    public void testJitter() {
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS
        SMAUtilsTest.testJitter(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, OSType.LINUX, 20);
        SMAUtilsTest.testJitter(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, OSType.WINDOWS, 20);
        SMAUtilsTest.testJitter(175, 10000, 10000, 25, 4, 1, TypeOfRIs.ZONAL, OSType.WINDOWS, 20);
    }

}
