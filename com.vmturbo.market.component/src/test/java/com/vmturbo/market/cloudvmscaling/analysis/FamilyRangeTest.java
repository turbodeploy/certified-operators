package com.vmturbo.market.cloudvmscaling.analysis;

import org.junit.Test;

import com.vmturbo.market.cloudvmscaling.entities.SMAPlatform;
import com.vmturbo.market.cloudvmscaling.entities.SMAStatistics.TypeOfRIs;

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
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
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
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, SMAPlatform.LINUX, familyRange);
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.REGIONAL, SMAPlatform.WINDOWS, familyRange);
        SMATestUtils.testRandomInput(nTemplates, vms, ris, families, zones, nBusinessAccounts, TypeOfRIs.ZONAL, SMAPlatform.WINDOWS, familyRange);
    }

}
