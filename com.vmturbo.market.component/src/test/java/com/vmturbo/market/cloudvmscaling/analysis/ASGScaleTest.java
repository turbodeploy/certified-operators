package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;

/**
 * Testing for Auto scaling group.
 */
public class ASGScaleTest {
    /**
     * call SMA with inputs that has ASG for zonal regional and isf.
     */
    @Test
    public void testAsgScale() {
        int familyRange = 20;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testASG(175, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange, 20, 20);
        SMATestUtils.testASG(175, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange, 20, 20);
        SMATestUtils.testASG(175, 25, 4, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange, 20, 20);
    }

    /**
     * test stability property of SMA when input has ASG.
     */
    @Ignore
    @Test
    public void testAsgStability() {
        int familyRange = 20;
        int[] loops = new int[15];
        for (int i = 0; i < 15; i++) {
            loops[i] = 0;
        }

        for (int i = 0; i < 100; i++) {
            // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
            SMATestUtils.testASGStability(175, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange, 100, 100, loops);
            SMATestUtils.testASGStability(175, 25, 4, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange, 1000, 100, loops);
            SMATestUtils.testASGStability(175, 25, 4, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange, 1000, 100, loops);
        }
    }

}
