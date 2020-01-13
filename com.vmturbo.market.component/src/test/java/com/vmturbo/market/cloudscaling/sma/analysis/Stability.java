package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Runs stability test. Run SMA. Execute all actions. Run SMA again and make sure there is no scaling.
 */
public class Stability {
    /**
     * run the stability test for regional, zonal and isf ris.
     */
    @Ignore
    @Test
    public void testStability() {
        int familyRange = 20;
        int[] loops = new int[15];
        for (int i = 0; i < 15; i++) {
            loops[i] = 0;
        }

        for (int i = 0; i < 100; i++) {
            // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
            //SMATestUtils.testStability(175, 1000, 1000, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange, loops);
            SMAUtilsTest.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, OSType.WINDOWS, familyRange, loops);
            SMAUtilsTest.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.ZONAL, OSType.WINDOWS, familyRange, loops);
            SMAUtilsTest.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, OSType.LINUX, familyRange, loops);
        }
    }

}
