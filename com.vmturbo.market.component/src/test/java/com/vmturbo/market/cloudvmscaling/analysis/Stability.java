package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;

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
            SMATestUtils.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange, loops);
            SMATestUtils.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange, loops);
            SMATestUtils.testStability(175, 10000, 10000, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange, loops);
        }
    }

}
