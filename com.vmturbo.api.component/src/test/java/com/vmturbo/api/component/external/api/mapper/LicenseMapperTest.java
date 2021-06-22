package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.licensing.utils.LicenseDeserializer;

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

        LicenseSummary maxReportEditorsCountLicenseSummary =
                LicenseSummary.newBuilder().setMaxReportEditorsCount(10).build();
        assertEquals(10, LicenseMapper.licenseSummaryToLicenseApiDTO(maxReportEditorsCountLicenseSummary)
                .getMaxReportEditorsCount().intValue());

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

    private static final String UUID = "a-b-c-d";
    private static final String FILENAME = "lic.xml";
    private static final String EMAIL = "joe@vmt.com";
    private static final String CUSTOMER_ID = "abcdEFG";
    private static final String EDITION = "Super";
    private static final String EXPIRATION = "2030-12-31";
    private static final String OWNER = "Joe B.";
    private static final String KEY = "key-123";
    private static final String EXTERNAL_KEY = "ext-key-123";
    private static final int NUM = 1000;
    private static final List<String> FEATURES = Lists.newArrayList("F1", "F2", "F3");

    /**
     * Increase code coverage.
     */
    @Test
    public void testMapper() {
        Licensing.LicenseDTO dto = Licensing.LicenseDTO.newBuilder()
                .setUuid(UUID)
                .setFilename(FILENAME)
                .setTurbo(Licensing.LicenseDTO.TurboLicense.newBuilder()
                        .setEmail(EMAIL)
                        .setCustomerId(CUSTOMER_ID)
                        .setEdition(EDITION)
                        .setExpirationDate(EXPIRATION)
                        .setLicenseOwner(OWNER)
                        .setLicenseKey(KEY)
                        .setExternalLicenseKey(EXTERNAL_KEY)
                        // This will be ignored
                        .setCountedEntity(ILicense.CountedEntity.SOCKET.name())
                        .setNumLicensedEntities(NUM)
                        .addAllFeatures(FEATURES)
                        .build())
                .build();
        LicenseApiDTO apiDto = LicenseMapper.licenseDTOtoLicenseApiDTO(dto);

        assertEquals(UUID, apiDto.getUuid());
        assertEquals(FILENAME, apiDto.getFilename());
        assertEquals(EMAIL, apiDto.getEmail());
        assertEquals(CUSTOMER_ID, apiDto.getCustomerId());
        assertEquals(EDITION, apiDto.getEdition());
        assertEquals(EXPIRATION, apiDto.getExpirationDate());
        assertEquals(OWNER, apiDto.getLicenseOwner());
        assertEquals(KEY, apiDto.getLicenseKey());
        assertEquals(EXTERNAL_KEY, apiDto.getExternalLicenseKey());
        // Always returns VM
        assertEquals(ILicense.CountedEntity.VM, apiDto.getCountedEntity());
        assertEquals(NUM, apiDto.getNumLicensedEntities());
        assertTrue(FEATURES.containsAll(apiDto.getFeatures()));
    }

    /**
     * Verify that customer ID is unset when unset or missing from the license.
     */
    @Test
    public void testDefaultCustomerId() {
        // Customer ID unset
        Licensing.LicenseDTO dto2 = Licensing.LicenseDTO.newBuilder()
                .setTurbo(Licensing.LicenseDTO.TurboLicense.newBuilder()
                        .build())
                .build();
        LicenseApiDTO apiDto2 = LicenseMapper.licenseDTOtoLicenseApiDTO(dto2);
        assertNull(apiDto2.getCustomerId());

        // Customer ID set to MISSING string
        Licensing.LicenseDTO dto1 = Licensing.LicenseDTO.newBuilder()
                .setTurbo(Licensing.LicenseDTO.TurboLicense.newBuilder()
                        .setCustomerId(LicenseDeserializer.CUSTOMER_ID_MISSING)
                        .build())
                .build();
        LicenseApiDTO apiDto1 = LicenseMapper.licenseDTOtoLicenseApiDTO(dto1);
        assertNull(apiDto1.getCustomerId());
    }
}
