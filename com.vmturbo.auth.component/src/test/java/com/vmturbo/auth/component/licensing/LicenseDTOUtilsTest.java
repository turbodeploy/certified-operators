package com.vmturbo.auth.component.licensing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;

/**
 * Some tests for LicenseDTOUtils.
 */
public class LicenseDTOUtilsTest {
    String email = "somebody@mail.com";
    List<String> features = Arrays.asList("Feature");
    Date tomorrow  = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    Date yesterday = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));

    /**
     * Test that the license summary is valid when all input licenses are valid.
     */
    @Test
    public void testCreateLicenseSummaryIsValid() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense1 = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                tomorrow, email, features, 1));

        LicenseDTO validLicense2 = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                tomorrow, email, features, 1));

        // summary is valid since both are valid
        assertTrue(LicenseDTOUtils.createLicenseSummary(ImmutableList.of(validLicense1, validLicense2),
                Optional.empty()).getIsValid());
    }

    /**
     * Test that the license summary is valid when at least one of the non-expired input licenses are invalid.
     */
    @Test
    public void testCreateLicenseSummaryInValid() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                tomorrow, email, features, 1));

        LicenseDTO invalidLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createInvalidLicense(
                tomorrow, email, features, 1));

        // the summary should now be invalid
        assertFalse(LicenseDTOUtils.createLicenseSummary(ImmutableList.of(validLicense, invalidLicense),
                Optional.empty()).getIsValid());
    }

    /**
     * Test that the license summary is valid even if there are invalid expired licenses present.
     */
    @Test
    public void testCreateLicenseSummaryValidWithExpired() {
        // give a set of two licenses -- both valid and not expired
        LicenseDTO validLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createLicense(
                tomorrow, email, features, 1));

        LicenseDTO invalidLicense = LicenseDTOUtils.iLicenseToLicenseDTO(LicenseTestUtils.createInvalidLicense(
                yesterday, email, features, 1));

        // the summary should now be invalid
        assertTrue(LicenseDTOUtils.createLicenseSummary(ImmutableList.of(validLicense, invalidLicense),
                Optional.empty()).getIsValid());
    }
}
