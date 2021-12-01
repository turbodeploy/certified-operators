package com.vmturbo.integrations.intersight.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.cisco.intersight.client.ApiClient;
import com.cisco.intersight.client.ApiException;
import com.cisco.intersight.client.JSON;
import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import reactor.core.publisher.Flux;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseCountUpdater.WorkloadCountInfo;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * Test for IntersightLicenseCountUpdater.
 */
public class IntersightLicenseCountUpdaterTest {

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
    private final IntersightLicenseClient intersightLicenseClient =
            mock(IntersightLicenseClient.class);
    private final IntersightLicenseSyncService licenseSyncService =
            mock(IntersightLicenseSyncService.class);
    private KeyValueStore kvStore = new MapKeyValueStore();
    private IntersightLicenseCountUpdater licenseCountUpdater;

    /**
     * A setup.
     *
     * @throws IOException should not happen in testing.
     */
    @Before
    public void setup() throws IOException {
        licenseCountUpdater = new IntersightLicenseCountUpdater(true, kvStore, licenseCheckClient,
                intersightLicenseClient, 60, licenseSyncService);
        ApiClient apiClient = mock(ApiClient.class);
        when(intersightLicenseClient.getApiClient()).thenReturn(apiClient);
        when(apiClient.getJSON()).thenReturn(new JSON());
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense("1", LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo invalidLicense =
                IntersightLicenseTestUtils.createIwoLicense("2", LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.NOTLICENSED);
        List<LicenseLicenseInfo> targetLicenses = Lists.newArrayList(validLicense, invalidLicense);
        when(licenseSyncService.getAvailableIntersightLicenses()).thenReturn(targetLicenses);
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns true when there is a
     * new workload count to publish.
     */
    @Test
    public void testShouldUpdateWorkloadCountNewCount() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo latestCountInfo = new WorkloadCountInfo(1, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense(lastMoid, LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertTrue(licenseCountUpdater.shouldUpdateWorkloadCounts(latestCountInfo, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is no
     * new count, and no change to target license moid.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoChange() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        String lastMoid = "1";
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense(lastMoid, LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertTrue(licenseCountUpdater.shouldUpdateWorkloadCounts(lastPublishedCountInfo,
                targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns true when there is no
     * new count but the target license moid has changed.
     */
    @Test
    public void testShouldUpdateWorkloadCountLicenseChange() {
        Instant testInstant = Instant.now();
        WorkloadCountInfo lastPublishedCountInfo = new WorkloadCountInfo(2, testInstant);
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense("2", LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        assertTrue(licenseCountUpdater.shouldUpdateWorkloadCounts(lastPublishedCountInfo,
                targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is no
     * count information yet, even though the target license moid has changed.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoValidCount() {
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense("2", LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        List<LicenseLicenseInfo> targetLicenses = Collections.singletonList(validLicense);

        // test a null count object
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(null, targetLicenses));

        // also check a count with no generation date
        WorkloadCountInfo newCountInfo = new WorkloadCountInfo(2, null);
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(newCountInfo, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater.shouldUpdateWorkloadCounts(..) returns false when there is a
     * workload count change, but no target license available.
     */
    @Test
    public void testShouldUpdateWorkloadCountNoTargetLicense() {
        Instant testInstant = Instant.now();
        // look at all the new workload!! So Exciting!!
        WorkloadCountInfo newCountInfo = new WorkloadCountInfo(10000, testInstant);
        List<LicenseLicenseInfo> targetLicenses = Collections.emptyList();

        // waa-wuh. The tree fell in the forest with noone there to hear it.
        assertFalse(licenseCountUpdater.shouldUpdateWorkloadCounts(newCountInfo, targetLicenses));
    }

    /**
     * Verify that LicenseCountUpdater will always upload license count when Intersight license
     * exists. No matter valid or not.
     *
     * @throws IOException  should not happen in testing.
     * @throws ApiException should not happen in testing.
     */
    @Test
    public void testAlwaysUpdateWorkloadCount() throws IOException, ApiException {
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense("1", LicenseTypeEnum.IWO_ESSENTIAL,
                        LicenseStateEnum.COMPLIANCE);
        licenseCountUpdater.uploadWorkloadCount(validLicense, 0);
        verify(intersightLicenseClient).updateLicenseLicenseInfo(any(), any());
    }

    /**
     * Verify that LicenseCountUpdater will not upload license count when valid Intersight license
     * doesn't exists.
     *
     * @throws IOException  should not happen in testing.
     * @throws ApiException should not happen in testing.
     */
    @Test
    public void testUpdateWorkloadCountWithoutIntersightLicense() throws IOException, ApiException {
        // LicenseTypeEnum.STANDARD is not a valid Intersight license
        LicenseLicenseInfo validLicense =
                IntersightLicenseTestUtils.createIwoLicense("1", LicenseTypeEnum.STANDARD,
                        LicenseStateEnum.COMPLIANCE);
        licenseCountUpdater.uploadWorkloadCount(validLicense, 0);
        verify(intersightLicenseClient, never()).updateLicenseLicenseInfo(any(), any());
    }

    /**
     * Verify that LicenseCountUpdater will only upload license count to the primary license.
     *
     * @throws IOException          should not happen in testing.
     * @throws ApiException         should not happen in testing.
     * @throws InterruptedException should not happen in testing.
     */
    @Test
    public void testOnlyUploadToPrimaryLicense()
            throws IOException, ApiException, InterruptedException {
        LicenseSummary licenseSummary = LicenseSummary.newBuilder()
                .setNumInUseEntities(100)
                .setGenerationDate("2020-11-26T16:58:52Z")
                .build();
        licenseCountUpdater.onLicenseSummaryUpdated(licenseSummary);
        licenseCountUpdater.syncLicenseCounts(false);
        verify(intersightLicenseClient, times(1)).updateLicenseLicenseInfo(any(), any());
    }

    /**
     * Test reactor will retry on error.
     *
     * @throws InterruptedException interrupted exception
     */
    @Ignore("Flux is currently removed")
    public void testReactorWillRetryOnError() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        Flux.interval(Duration.ofMillis(250))
                .map(input -> {
                    counter.getAndIncrement();
                    if (input < 1) {
                        return "tick " + input;
                    }
                    throw new RuntimeException("boom: " + input);
                })
                .doOnError(err -> System.out.println(
                        "Error encountered while subscribing license summary update events: "
                                + err.getMessage()))
                .retry()
                .elapsed()
                .subscribe(System.out::println);

        Thread.sleep(2100);
        assertEquals(4, counter.get());
    }
}
