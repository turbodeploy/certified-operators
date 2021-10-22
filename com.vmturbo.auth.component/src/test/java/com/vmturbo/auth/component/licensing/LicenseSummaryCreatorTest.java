package com.vmturbo.auth.component.licensing;

import static com.vmturbo.auth.component.licensing.LicenseTestUtils.createExternalLicense;
import static com.vmturbo.auth.component.licensing.LicenseTestUtils.createGrafanaJwtToken;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Unit tests for {@link LicenseSummaryCreator}.
 */
public class LicenseSummaryCreatorTest {
    private static final String EMAIL = "somebody@mail.com";
    private static final List<String> FEATURES = Collections.singletonList("Feature");
    private static final Date TOMORROW = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    private static final Date YESTERDAY = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));

    private static final int DEFAULT_MAX_REPORT_EDITORS = 10;

    private LicenseSummaryCreator creator = new LicenseSummaryCreator(DEFAULT_MAX_REPORT_EDITORS);

    /**
     * Test that the license summary is valid when all input licenses are valid.
     */
    @Test
    public void testCreateLicenseSummaryIsValid() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense1 = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                TOMORROW, EMAIL, FEATURES, 1));

        LicenseDTO validLicense2 = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                TOMORROW, EMAIL, FEATURES, 1));

        // summary is valid since both are valid
        assertTrue(creator.createLicenseSummary(
                ImmutableList.of(validLicense1, validLicense2),
                Optional.empty()).getIsValid());
    }

    /**
     * Test that the license summary is valid when at least one of the non-expired input licenses are invalid.
     */
    @Test
    public void testCreateLicenseSummaryInValid() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                TOMORROW, EMAIL, FEATURES, 1));

        LicenseDTO invalidLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createInvalidLicense(
                TOMORROW, EMAIL, FEATURES, 1));

        // the summary should now be invalid
        assertFalse(creator.createLicenseSummary(ImmutableList.of(validLicense, invalidLicense),
                Optional.empty()).getIsValid());
    }

    /**
     * Test that the license summary is valid even if there are invalid expired licenses present.
     */
    @Test
    public void testCreateLicenseSummaryValidWithExpired() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                TOMORROW, EMAIL, FEATURES, 1));

        LicenseDTO invalidLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createInvalidLicense(
                YESTERDAY, EMAIL, FEATURES, 1));

        // the summary should now be invalid
        assertTrue(creator.createLicenseSummary(ImmutableList.of(validLicense, invalidLicense),
                Optional.empty()).getIsValid());
    }

    /**
     * Test that the license summary has information about max count of allowed report editors
     * when grafana license contains this info.
     */
    @Test
    public void testCreateLicenseSummaryWithReportEditorsDetails() {
        // ARRANGE
        final int grafanaReportEditorCount = 3;
        final LicenseDTO grafanaLicense =
                createExternalLicense(Type.GRAFANA, createGrafanaJwtToken(grafanaReportEditorCount),
                        LocalDateTime.MAX);

        // ACT
        LicenseSummary licenseSummary =
                creator.createLicenseSummary(Collections.singletonList(grafanaLicense),
                        Optional.empty());

        // ASSERT
        Assert.assertEquals(grafanaReportEditorCount, licenseSummary.getMaxReportEditorsCount());
    }

    /**
     * Test that the license summary max report editors count gets set to the default number of
     * editors when grafana license doesn't have this info or if there is no applied grafana
     * license.
     */
    @Test
    public void testCreateLicenseSummaryWithoutReportEditorsDetails() {
        // ARRANGE
        final LicenseDTO grafanaLicenseWithoutEditorsCount =
                createExternalLicense(Type.GRAFANA, createGrafanaJwtToken(null), LocalDateTime.MAX);
        final LicenseDTO unknownExternalLicense = LicenseDTO.newBuilder()
                .setExternal(ExternalLicense.newBuilder().setType(Type.UNKNOWN).build())
                .build();

        // ACT
        final LicenseSummary licenseSummary1 = creator.createLicenseSummary(
                Collections.singletonList(grafanaLicenseWithoutEditorsCount), Optional.empty());
        final LicenseSummary licenseSummary2 = creator.createLicenseSummary(
                Collections.singletonList(unknownExternalLicense), Optional.empty());

        // ASSERT
        Assert.assertEquals(DEFAULT_MAX_REPORT_EDITORS, licenseSummary1.getMaxReportEditorsCount());
        Assert.assertEquals(DEFAULT_MAX_REPORT_EDITORS, licenseSummary2.getMaxReportEditorsCount());
    }

    /**
     * Test that we don't consider the max count of allowed reports from expired grafana license.
     */
    @Test
    public void testCreateLicenseSummaryWhenGrafanaLicenseIsExpired() {
        // ARRANGE
        final LicenseDTO expiredGrafanaLicense =
                createExternalLicense(Type.GRAFANA, createGrafanaJwtToken(2),
                        LocalDateTime.of(2020, Month.FEBRUARY, 1, 1, 1));

        // ACT
        final LicenseSummary licenseSummary = creator.createLicenseSummary(
                Collections.singletonList(expiredGrafanaLicense), Optional.empty());

        // ASSERT
        Assert.assertEquals(DEFAULT_MAX_REPORT_EDITORS, licenseSummary.getMaxReportEditorsCount());
    }

}
