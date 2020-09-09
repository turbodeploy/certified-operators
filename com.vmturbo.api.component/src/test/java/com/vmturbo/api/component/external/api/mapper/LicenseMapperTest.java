package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.junit.Test;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Tests for LicenseMapper.
 */
public class LicenseMapperTest {

    /**
     * Verify that the isValid flag is based directly off of the LicenseSummary.isValid attributes,
     * rather than using the ILicense.isValid() dynamic calculation based on the presence of errors.
     */
    @Test
    public void testLicenseSummaryToLicenseApiDTOIsValid() {
        LicenseSummary validLicenseSummary = LicenseSummary.newBuilder().setIsValid(true).build();
        assertTrue(LicenseMapper.licenseSummaryToLicenseApiDTO(validLicenseSummary).isValid());

        LicenseSummary invalidLicenseSummary = LicenseSummary.newBuilder().setIsValid(false).build();
        assertFalse(LicenseMapper.licenseSummaryToLicenseApiDTO(invalidLicenseSummary).isValid());
    }


    /**
     * Verify that the isExpired flag is based directly off of the LicenseSummary.isExpired attributes,
     * rather than using the ILicense.isExpired() dynamic calculation based on the expirationDate field.
     */
    @Test
    public void testLicenseSummaryToLicenseApiDTOIsExpire() {
        String tomorrow  = DateTimeUtil.formatDate(Date.from(Instant.now().plus(1, ChronoUnit.DAYS)));
        LicenseSummary validLicenseSummary = LicenseSummary.newBuilder()
                .setIsExpired(true)
                .setExpirationDate(tomorrow)
                .build();
        // should be "expired" even if the date field says otherwise.
        assertTrue(LicenseMapper.licenseSummaryToLicenseApiDTO(validLicenseSummary).isValid());

        String yesterday = DateTimeUtil.formatDate(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        LicenseSummary invalidLicenseSummary = LicenseSummary.newBuilder()
                .setIsExpired(false)
                .setExpirationDate(yesterday)
                .build();
        // should not be expired, even if the date is before today.
        assertFalse(LicenseMapper.licenseSummaryToLicenseApiDTO(invalidLicenseSummary).isExpired());
    }
}
