package com.vmturbo.auth.api.licensing;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Tests involving the license check client.
 */
public class LicenseCheckClientTest {
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
