package com.vmturbo.auth.api.licensing;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

/**
 * Tests involving the license check client.
 */
public class LicenseCheckClientTest {

    private LicenseCheckClient licenseCheckClient;
    private SenderReceiverPair<LicenseSummary> summaryReceiver = new SenderReceiverPair<>();
    private ExecutorService executorService = mock(ExecutorService.class);
    private long licenseTimeoutMs = 50;

    @Before
    public void setup() {
        licenseCheckClient = new LicenseCheckClient(summaryReceiver, executorService, null, licenseTimeoutMs);
    }

    /**
     * Test simple cases of getLicenseSummary()
     */
    @Test
    public void testGetLicenseSummary() {
        summaryReceiver.sendMessage(LicenseSummary.getDefaultInstance());
        assertTrue(licenseCheckClient.isReady());
        LicenseSummary licenseSummary = licenseCheckClient.getLicenseSummary();
        assertNotNull(licenseSummary);
        assertEquals(LicenseSummary.getDefaultInstance(), licenseSummary);
    }

    /**
     * Verify getLicenseSummary() behavior when a license summary has not been received yet
     */
    @Test
    public void testGetLicenseSummaryWhenNotReady() {
        // we haven't sent a summary yet, so the license check client should not be ready
        assertFalse(licenseCheckClient.isReady());
        Instant startTime = Instant.now();
        // when we try to get the status, we should get a LicenseCheckNotReadyException after our
        // timeout period elapses
        boolean exceptionWasThrown = false;
        Instant finishTime;
        LicenseSummary licenseSummary;
        try {
            licenseSummary = licenseCheckClient.getLicenseSummary();
        } catch (LicenseCheckNotReadyException lcnre) {
            exceptionWasThrown = true;
        } finally {
            finishTime = Instant.now();
        }
        assertTrue("LicenseCheckNotReadyException should be thrown since no summary available", exceptionWasThrown);
        // disabling the time taken check, since it's not working reliably in Jenkins
/*
        Duration timeTaken = Duration.between(startTime, finishTime);
        assertTrue("license check should fail after at least "+ licenseTimeoutMs +" timeout period, but took "+ timeTaken.toMillis(),
                timeTaken.toMillis() > licenseTimeoutMs);
*/
        // now we'll send a license summary in and the same check should succeed
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.PLANNER.getKey())
                .build());
        licenseSummary = licenseCheckClient.getLicenseSummary();
        assertNotNull("isFeatureAvailable should work after first license summary received.", licenseSummary);
    }

    /**
     * Test simple cases of isFeatureAvailable()
     */
    @Test
    public void testIsFeatureAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .build());
        // make sure the license check client is ready
        assertTrue(licenseCheckClient.isReady());
        assertTrue("Check passes when requested feature is available",
                licenseCheckClient.isFeatureAvailable(ProbeLicense.APP_CONTROL));
        assertFalse("Fail is requested feature is not available",
                licenseCheckClient.isFeatureAvailable(ProbeLicense.PLANNER));
    }

    /**
     * Verify isFeatureAvailable() functionality when license summary not available (yet)
     */
    @Test
    public void testIsFeatureAvailableWhenNoLicenseSummary() {
        // we'll remove the timeout to speed up this test. We already tested timeout in the
        // testGetLicenseSummaryWhenNotReady() test.
        licenseCheckClient = new LicenseCheckClient(summaryReceiver, executorService, null, 0);
        // we haven't sent a summary yet, so the license check client should not be ready
        assertFalse(licenseCheckClient.isReady());
        boolean exceptionWasThrown = false;
        try {
            licenseCheckClient.isFeatureAvailable(ProbeLicense.PLANNER);
        } catch (LicenseCheckNotReadyException lcnre) {
            exceptionWasThrown = true;
        }
        assertTrue("LicenseCheckNotReadyException should be thrown since no summary available", exceptionWasThrown);
        // now we'll send a license summary in and the same check should succeed
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.PLANNER.getKey())
                .build());
        assertTrue("isFeatureAvailable should work after first license summary received.", licenseCheckClient.isFeatureAvailable(ProbeLicense.PLANNER));
    }

    /**
     * Test the areFeaturesAvailable() method when checks should pass
     */
    @Test
    public void testAreFeaturesAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .addFeature(ProbeLicense.ACTION_SCRIPT.getKey())
                .addFeature(ProbeLicense.CONTAINER_CONTROL.getKey())
                .build());
        // make sure the license check client is ready
        assertTrue(licenseCheckClient.isReady());
        assertTrue("Check passes when all required features are available",
                licenseCheckClient.areFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.ACTION_SCRIPT)));
        assertTrue("Check passes when no required features",
                licenseCheckClient.areFeaturesAvailable(Collections.emptySet()));
    }

    /**
     * Test the areFeaturesAvailable() method when a required feature is missing.
     */
    @Test
    public void testAreFeaturesAvailableMissingFeature() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .addFeature(ProbeLicense.CONTAINER_CONTROL.getKey())
                .build());
        // make sure the license check client is ready
        assertTrue(licenseCheckClient.isReady());
        assertFalse("Check passes when all required features are available",
                licenseCheckClient.areFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.ACTION_SCRIPT)));
        assertTrue("Check passes when no required features",
                licenseCheckClient.areFeaturesAvailable(Collections.emptySet()));
    }

    /**
     * Test that the method to check when features available quietly succeeds when all requested
     * features are present in th eactive licenses.
     */
    @Test
    public void testCheckFeatureAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .build());
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.STORAGE);
    }

    /**
     * Test that the method to check when features available quietly succeeds when all requested
     * features are present in th eactive licenses.
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void testCheckFeatureNotAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .build());
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.ACTION_SCRIPT);
    }

    /**
     * Test that the method to check when features available quietly succeeds when all requested
     * features are present in th eactive licenses.
     */
    @Test
    public void testCheckFeaturesAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .addFeature(ProbeLicense.ACTION_SCRIPT.getKey())
                .addFeature(ProbeLicense.CONTAINER_CONTROL.getKey())
                .build());
        // make sure the license check client is ready
        assertTrue(licenseCheckClient.isReady());
        licenseCheckClient.checkFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.ACTION_SCRIPT));
    }

    /**
     * Test the checkFeaturesAvailable() method when a required feature is missing.
     */
    @Test(expected = LicenseFeaturesRequiredException.class)
    public void testCheckFeaturesNotAvailable() {
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.APP_CONTROL.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .addFeature(ProbeLicense.CONTAINER_CONTROL.getKey())
                .build());
        // make sure the license check client is ready
        assertTrue(licenseCheckClient.isReady());
        // this check should generate a LicenseFeaturesRequiredException
        licenseCheckClient.checkFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.ACTION_SCRIPT));
    }

    /**
     * Verify checkFeaturesAvailable() functionality when license summary not available (yet)
     */
    @Test
    public void testCheckFeaturesAvailableWhenNotReady() {
        // we'll remove the timeout to speed up this test. We already tested timeout in the
        // testGetLicenseSummaryWhenNotReady() test.
        licenseCheckClient = new LicenseCheckClient(summaryReceiver, executorService, null, 0);
        // we haven't sent a summary yet, so the license check client should not be ready
        assertFalse(licenseCheckClient.isReady());
        boolean exceptionWasThrown = false;
        try {
            licenseCheckClient.checkFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.PLANNER));
        } catch (LicenseCheckNotReadyException lcnre) {
            exceptionWasThrown = true;
        }
        assertTrue("LicenseCheckNotReadyException should be thrown since no summary available", exceptionWasThrown);
        // now we'll send a license summary in and the same check should succeed
        summaryReceiver.sendMessage(LicenseSummary.newBuilder()
                .addFeature(ProbeLicense.PLANNER.getKey())
                .addFeature(ProbeLicense.STORAGE.getKey())
                .build());
        licenseCheckClient.checkFeaturesAvailable(ImmutableSet.of(ProbeLicense.STORAGE, ProbeLicense.PLANNER));
    }



    /**
     * Test the license summary change detection method will trigger when the summary has changed
     * AND the generation date is newer.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerNewerAndDifferent() {
        //
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T11:32:40Z")
                .build();
        assertTrue(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will still trigger when the generation date
     * is newer but the other summary contents have not changed. (this is expected behavior but we
     * may change this later since the generation date alone isn't that important)
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerJustNewer() {
        //
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T11:32:40Z")
                .build();
        assertTrue(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will still trigger when the generation date
     * is newer even though the timezone may have changed the generation date timestamp offset.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerTimezoneChange() {
        //
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        // the second summary has an earlier wall clock time but with the offset is a later timestamp
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T09:32:40-02:00")
                .build();
        assertTrue(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will NOT trigger when the generation date of
     * the incoming summary is older than the current last-seen summary even if there are other
     * changes to the summary.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerOlder() {
        //
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T09:32:40Z")
                .build();
        assertFalse(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will see identical license summaries as the
     * same.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerNoChange() {
        //
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        assertFalse(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will see identical license summaries as the
     * same even if there are no generation dates to compare.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerNoDates() {
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .build();
        assertFalse(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will detect the summary as newer if the new
     * summary has a generation date and the last known summary does not.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerBeforeNoDate() {
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        assertTrue(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }

    /**
     * Test the license summary change detection method will NOT detect the summary as newer if the
     * new summary has a generation date and the last known summary does.
     */
    @Test
    public void testIsLicenseSummaryDifferentAndNewerAfterNoDate() {
        LicenseSummary a = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A", "B"))
                .setGenerationDate("2020-04-08T10:32:40Z")
                .build();
        LicenseSummary b = LicenseSummary.newBuilder()
                .addAllFeature(ImmutableList.of("A"))
                .build();
        assertFalse(LicenseCheckClient.isLicenseSummaryDifferentAndNewer(a, b));
    }
}
