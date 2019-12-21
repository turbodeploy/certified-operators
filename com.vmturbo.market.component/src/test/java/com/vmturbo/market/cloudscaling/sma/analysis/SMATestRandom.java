package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAPlatform;
import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;

/**
 * Generate random inputs and test the results.
 */
public class SMATestRandom {

    /**
     * Validate that the testRandomInput generator is generating all the providers
     * in the family.
     */
    @Test
    public void testRandomInputOneVm() {
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testRandomInput(5, 1, 1, 1, 1, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, 1);
        SMATestUtils.testRandomInput(5, 1, 1, 1, 1, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, 1);
        SMATestUtils.testRandomInput(5, 1, 1, 1, 1, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, 1);
    }

    /**
     * test the running time for worst case input scenario for SMA running time.
     */
    @Test
    public void testWorstCase() {
        // templates, VMs, RIs, families, couponRange, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testWorstCase(80, 80);
    }


    /**
     * Test for single zone. 6000 vms and 100 ris.
     */
    @Test
    public void testRandomInputSmall() {
        int vms = 6000;
        int ris = 100;
        int families = 10;
        int zones = 1;
        int familyRange = 5;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testRandomInput(5, vms, ris, families, zones, 1, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(5, vms, ris, families, zones, 1, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(5, vms, ris, families, zones, 1, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
    }

    /**
     * test for 6000 vms and 100 ris 4 zones.
     */
    @Test
    public void testRandomInput6000VMs100RIs() {
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        int vms = 6000;
        int ris = 100;
        int families = 10;
        int zones = 4;
        int familyRange = 5;
        SMATestUtils.testRandomInput(20, vms, ris, families, zones, 8, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(20, vms, ris, families, zones, 8, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(20, vms, ris, families, zones, 8, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
    }

    /**
     *  test for 6 vms and 2 ris 4 zones.
     */
    @Test
    public void testRandomInput6VMs2RIs10familyRange() {
        int vms = 6;
        int ris = 2;
        int zones = 4;
        int families = 10;
        int familyRange = 10;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMATestUtils.testRandomInput(40, vms, ris, families, zones, 8, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(40, vms, ris, families, zones, 8, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(40, vms, ris, families, zones, 8, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
    }

}
