package com.vmturbo.integrations.intersight.licensing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.licensing.utils.CWOMLicenseEdition;

/**
 * Tests of the IntersightLicenseUtils.
 */
public class IntersightLicenseUtilsTest {

    /**
     * Test that IWO license editions are recognized as intersight licenses.
     */
    @Test
    public void testIsIntersightLicense() {
        for (IntersightLicenseEdition licenseEdition: IntersightLicenseEdition.values()) {
            LicenseDTO license = LicenseDTO.newBuilder()
                    .setEdition(licenseEdition.name())
                    .build();
            assertTrue(IntersightLicenseUtils.isIntersightLicense(license));
        }
    }

    /**
     * Verify that a non-IWO license is not recognized as an Intersight license.
     */
    @Test
    public void testIsIntersightLicenseNegative() {
        LicenseDTO license = LicenseDTO.newBuilder()
                .setEdition("Advanced")
                .build();
        assertFalse(IntersightLicenseUtils.isIntersightLicense(license));
    }

    /**
     * Test mapping of the intersight Essentials license.
     */
    @Test
    public void testToProxyEssentialsLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightLicenseEdition.IWO_ESSENTIALS.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test mapping of the intersight Advantage license.
     */
    @Test
    public void testToProxyAdvantageLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ADVANTAGE);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightLicenseEdition.IWO_ADVANTAGE.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_ADVANCED.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test mapping of the intersight Premier license.
     */
    @Test
    public void testToProxyPremiereLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.PREMIER);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightLicenseEdition.IWO_PREMIER.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_PREMIER.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test NON-mapping of the intersight Base license. We don't treat this as an error -- YET. For
     * now, we expect this to get mapped to an Essentials license. In the future, this may generate
     * an error or map to a null license.
     */
    @Test
    public void testToProxyBaseLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.BASE);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightLicenseEdition.IWO_ESSENTIALS.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is OUTOFCOMPLIANCE should be treated as still active. In this state, they are
     * in the "grace period".
     */
    @Test
    public void testToProxyGracePeriodLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.OUTOFCOMPLIANCE);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is GRACEEXPIRED should be treated as immediately inactive.
     */
    @Test
    public void testToProxyOutOfGracePeriodLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.GRACEEXPIRED);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertFalse(mappedLicense.getIsValid());
        assertTrue(StringUtils.isBlank(mappedLicense.getExpirationDate()));
    }


    /**
     * A license that is TRIALPERIOD should be treated as active.
     */
    @Test
    public void testToProxyTrialPeriodLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.TRIALPERIOD);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is TRIALEXPIRED should be treated as immediately inactive.
     */
    @Test
    public void testToProxyTrialExpiredLicense() {
        LicenseLicenseInfo essentialsLicense = mock(LicenseLicenseInfo.class);
        when(essentialsLicense.getMoid()).thenReturn("1");
        when(essentialsLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(essentialsLicense.getLicenseState()).thenReturn(LicenseStateEnum.TRIALEXPIRED);

        LicenseDTO mappedLicense = IntersightLicenseUtils.toProxyLicense(essentialsLicense);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertFalse(mappedLicense.getIsValid());
        assertTrue(StringUtils.isBlank(mappedLicense.getExpirationDate()));
    }

    /**
     * Verify that the proxy license comparison ignores uuid.
     */
    @Test
    public void testAreProxyLicensesEqual() {
        // license "A" is an "existing" license that has had an uuid assigned to it.
        // license "B" will be the same major license fields but no uuid yet.
        LicenseLicenseInfo iwoLicenseA = mock(LicenseLicenseInfo.class);
        when(iwoLicenseA.getMoid()).thenReturn("1");
        when(iwoLicenseA.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(iwoLicenseA.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);
        LicenseDTO licenseB = IntersightLicenseUtils.toProxyLicense(iwoLicenseA);
        LicenseDTO licenseA = licenseB.toBuilder()
                .setUuid("1x0101010")
                .build();

        assertTrue(IntersightLicenseUtils.areProxyLicensesEqual(licenseA, licenseB));
    }

    /**
     * Verify that the proxy license comparison picks up material state changes (e.g. expiration).
     */
    @Test
    public void testAreProxyLicensesEqualStateChanged() {
        LicenseLicenseInfo iwoLicense = mock(LicenseLicenseInfo.class);
        when(iwoLicense.getMoid()).thenReturn("1");
        when(iwoLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(iwoLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);
        LicenseDTO licenseA = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        when(iwoLicense.getLicenseState()).thenReturn(LicenseStateEnum.GRACEEXPIRED);
        LicenseDTO licenseB = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        assertFalse(IntersightLicenseUtils.areProxyLicensesEqual(licenseA, licenseB));
    }

    /**
     * Verify that the proxy license comparison picks up a license type change.
     */
    @Test
    public void testAreProxyLicensesEqualEditionChanged() {
        LicenseLicenseInfo iwoLicense = mock(LicenseLicenseInfo.class);
        when(iwoLicense.getMoid()).thenReturn("1");
        when(iwoLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ESSENTIAL);
        when(iwoLicense.getLicenseState()).thenReturn(LicenseStateEnum.COMPLIANCE);
        LicenseDTO licenseA = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        when(iwoLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ADVANTAGE);
        LicenseDTO licenseB = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        assertFalse(IntersightLicenseUtils.areProxyLicensesEqual(licenseA, licenseB));
    }
}
