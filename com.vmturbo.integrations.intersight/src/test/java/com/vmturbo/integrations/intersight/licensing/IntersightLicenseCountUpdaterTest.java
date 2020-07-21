package com.vmturbo.integrations.intersight.licensing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseCountUpdater.WorkloadCountInfo;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * Test for IntersightLicenseCountUpdater.
 */
public class IntersightLicenseCountUpdaterTest {

    private KeyValueStore kvStore = new MapKeyValueStore();

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final IntersightLicenseClient intersightLicenseClient = mock(IntersightLicenseClient.class);

    private final IntersightLicenseSyncService licenseSyncService = mock(IntersightLicenseSyncService.class);

    private IntersightLicenseCountUpdater licenseCountUpdater;

    /**
     * A setup.
     */
    @Before
    public void setup() {
        licenseCountUpdater = new IntersightLicenseCountUpdater(true,
                kvStore, licenseCheckClient, intersightLicenseClient, 60, licenseSyncService);
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns true when there is a
     * new workload count to publish.
     */
    @Test
    public void testShouldUpdateWorkloadCountNewCount() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        WorkloadCountInfo latestCountInfo = new WorkloadCountInfo(1, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense = IntersightLicenseTestUtils.createIwoLicense(lastMoid, LicenseTypeEnum.IWO_ESSENTIAL, LicenseStateEnum.COMPLIANCE );
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertTrue(licenseCountUpdater.shouldUpdateWorkloadCounts(latestCountInfo, lastPublishedCountInfo,
            lastMoid, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is no new
     * count, and no change to target license moid.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoChange() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense = IntersightLicenseTestUtils.createIwoLicense(lastMoid, LicenseTypeEnum.IWO_ESSENTIAL, LicenseStateEnum.COMPLIANCE );
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(lastPublishedCountInfo, lastPublishedCountInfo,
                lastMoid, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns true when there is no new
     * count but the target license moid has changed.
     */
    @Test
    public void testShouldUpdateWorkloadCountLicenseChange() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense = IntersightLicenseTestUtils.createIwoLicense("2", LicenseTypeEnum.IWO_ESSENTIAL, LicenseStateEnum.COMPLIANCE );
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertTrue(licenseCountUpdater.shouldUpdateWorkloadCounts(lastPublishedCountInfo, lastPublishedCountInfo,
                lastMoid, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is no
     * count information yet, even though the target license moid has changed.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoValidCount() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense = IntersightLicenseTestUtils.createIwoLicense("2", LicenseTypeEnum.IWO_ESSENTIAL, LicenseStateEnum.COMPLIANCE );
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        // test a null count object
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(null, lastPublishedCountInfo,
                lastMoid, targetLicenses));

        // also check a count with no generation date
        WorkloadCountInfo newCountInfo = new WorkloadCountInfo(2, null);
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(newCountInfo, lastPublishedCountInfo,
                lastMoid, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is
     * a workload count change, but no target license available.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoTargetLicense() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        // look at all the new workload!! So Exciting!!
        WorkloadCountInfo newCountInfo = new WorkloadCountInfo(10000, testInstant);
        String lastMoid = "1";
        List<LicenseLicenseInfo> targetLicenses = Collections.emptyList();

        // waa-wuh. The tree fell in the forest with noone there to hear it.
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(newCountInfo, lastPublishedCountInfo,
                lastMoid, targetLicenses));

    }

}
