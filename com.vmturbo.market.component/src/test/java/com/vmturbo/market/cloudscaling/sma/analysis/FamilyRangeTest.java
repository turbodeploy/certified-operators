package com.vmturbo.market.cloudscaling.sma.analysis;

import org.junit.Test;

import com.vmturbo.market.cloudscaling.sma.entities.SMAStatistics.TypeOfRIs;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Test the template generation.
 */
public class FamilyRangeTest {
    /**
     * Test one family and a range of one.
     */
    @Test
    public void testOneFamilyRangeOfOne() {
        int nTemplates = 20;
        int vms = 5;
        int ris = 4;
        int zones = 3;
        int nBusinessAccounts = 1;
        int families = 1;
        int familyRange = 1;
        // templates, VMs, RIs, families, zones, accounts,  OS, familyRange
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, OSType.LINUX, familyRange);
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, OSType.WINDOWS, familyRange);
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.ZONAL, OSType.WINDOWS, familyRange);
    }

    /**
     * Test 10 family and a range of 10.
     */
    @Test
    public void testTenFamilyRangeOfTen() {
        int nTemplates = 20;
        int vms = 6000;
        int ris = 100;
        int zones = 4;
        int nBusinessAccounts = 1;
        int families = 10;
        int familyRange = 10;
        // templates, VMs, RIs, families, zones, accounts, isZonalRIs, OS, familyRange
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, OSType.LINUX, familyRange);
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, OSType.WINDOWS, familyRange);
        SMAUtilsTest.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.ZONAL, OSType.WINDOWS, familyRange);
    }

}
