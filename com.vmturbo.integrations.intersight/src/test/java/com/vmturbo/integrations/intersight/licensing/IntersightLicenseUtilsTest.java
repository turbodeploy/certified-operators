package com.vmturbo.integrations.intersight.licensing;

import static com.vmturbo.integrations.intersight.licensing.IntersightLicenseTestUtils.createIwoLicense;
import static com.vmturbo.integrations.intersight.licensing.IntersightLicenseTestUtils.createProxyLicense;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.cisco.intersight.client.model.LicenseLicenseInfo;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseStateEnum;
import com.cisco.intersight.client.model.LicenseLicenseInfo.LicenseTypeEnum;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.integrations.intersight.licensing.IntersightLicenseUtils.BestAvailableIntersightLicenseComparator;
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
        for (IntersightProxyLicenseEdition licenseEdition: IntersightProxyLicenseEdition.values()) {
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
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.COMPLIANCE);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightProxyLicenseEdition.IWO_ESSENTIALS.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test mapping of the intersight Advantage license.
     */
    @Test
    public void testToProxyAdvantageLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ADVANTAGE, LicenseStateEnum.COMPLIANCE);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightProxyLicenseEdition.IWO_ADVANTAGE.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_ADVANCED.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test mapping of the intersight Premier license.
     */
    @Test
    public void testToProxyPremiereLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.PREMIER, LicenseStateEnum.COMPLIANCE);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightProxyLicenseEdition.IWO_PREMIER.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
        assertTrue(CWOMLicenseEdition.CWOM_PREMIER.getFeatures().containsAll(mappedLicense.getFeaturesList()));
    }

    /**
     * Test mapping of the intersight Base license.
     */
    @Test
    public void testToProxyBaseLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.BASE, LicenseStateEnum.COMPLIANCE);
        assertEquals("1", mappedLicense.getExternalLicenseKey());
        assertEquals(IntersightProxyLicenseEdition.IWO_BASE.name(), mappedLicense.getEdition());
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is OUTOFCOMPLIANCE should be treated as still active. In this state, they are
     * in the "grace period".
     */
    @Test
    public void testToProxyGracePeriodLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.OUTOFCOMPLIANCE);
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is GRACEEXPIRED should be treated as immediately inactive.
     */
    @Test
    public void testToProxyOutOfGracePeriodLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.GRACEEXPIRED);
        assertFalse(mappedLicense.getIsValid());
        assertTrue(StringUtils.isBlank(mappedLicense.getExpirationDate()));
    }


    /**
     * A license that is TRIALPERIOD should be treated as active.
     */
    @Test
    public void testToProxyTrialPeriodLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.TRIALPERIOD);
        assertEquals(ILicense.PERM_LIC, mappedLicense.getExpirationDate());
    }

    /**
     * A license that is TRIALEXPIRED should be treated as immediately inactive.
     */
    @Test
    public void testToProxyTrialExpiredLicense() {
        LicenseDTO mappedLicense = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.TRIALEXPIRED);
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
        LicenseLicenseInfo iwoLicenseA = createIwoLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.GRACEEXPIRED);
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
        LicenseLicenseInfo iwoLicense = createIwoLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.COMPLIANCE);
        LicenseDTO licenseA = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        when(iwoLicense.getLicenseState()).thenReturn(LicenseStateEnum.GRACEEXPIRED);
        LicenseDTO licenseB = createProxyLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.GRACEEXPIRED);

        assertFalse(IntersightLicenseUtils.areProxyLicensesEqual(licenseA, licenseB));
    }

    /**
     * Verify that the proxy license comparison picks up a license type change.
     */
    @Test
    public void testAreProxyLicensesEqualEditionChanged() {
        LicenseLicenseInfo iwoLicense = createIwoLicense("1", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.COMPLIANCE);
        LicenseDTO licenseA = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        when(iwoLicense.getLicenseType()).thenReturn(LicenseTypeEnum.ADVANTAGE);
        LicenseDTO licenseB = IntersightLicenseUtils.toProxyLicense(iwoLicense);

        assertFalse(IntersightLicenseUtils.areProxyLicensesEqual(licenseA, licenseB));
    }

    /**
     * Verify that if there are multiple active IWO licenses, we can identify the best one.
     */
    @Test
    public void testBestActiveLicenseComparatorByEdition() {
        // create three active licenses. The "premier" is the best one and is in the middle.
        LicenseLicenseInfo essential = createIwoLicense("1", LicenseTypeEnum.ESSENTIAL,LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo advantage = createIwoLicense("2", LicenseTypeEnum.ADVANTAGE,LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo premier = createIwoLicense("3", LicenseTypeEnum.PREMIER,LicenseStateEnum.COMPLIANCE);

        List<LicenseLicenseInfo> licenses = Arrays.asList(advantage, premier, essential);
        licenses.sort(new BestAvailableIntersightLicenseComparator());
        // they should be in order of best -> worst editions
        assertEquals("3", licenses.get(0).getMoid());
        assertEquals("2", licenses.get(1).getMoid());
        assertEquals("1", licenses.get(2).getMoid());
    }

    /**
     * Verify that if there are two licenses with the same edition but different active states, the
     * active one is higher in the list.
     */
    @Test
    public void testBestActiveLicenseComparatorMixedStates() {
        LicenseLicenseInfo inactivePremier = createIwoLicense("1", LicenseTypeEnum.PREMIER, LicenseStateEnum.GRACEEXPIRED);
        LicenseLicenseInfo activePremier = createIwoLicense("2", LicenseTypeEnum.PREMIER, LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo activeEssential = createIwoLicense("3", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.COMPLIANCE);

        List<LicenseLicenseInfo> licenses = Arrays.asList(activePremier, activeEssential, inactivePremier);
        licenses.sort(new BestAvailableIntersightLicenseComparator());
        // they should be in order of: inactive premier, active essential, active premier
        assertEquals("2", licenses.get(0).getMoid());
        assertEquals("3", licenses.get(1).getMoid());
        assertEquals("1", licenses.get(2).getMoid());
    }

    /**
     * Verify that if there are two licenses with the same edition and effective states, the moid determines
     * order.
     */
    @Test
    public void testBestActiveLicenseComparatorMoidCheck() {
        // even though the states are different, they are all considered "active" and equal candidates
        // for "best license available".
        LicenseLicenseInfo license1 = createIwoLicense("1", LicenseTypeEnum.PREMIER, LicenseStateEnum.TRIALPERIOD);
        LicenseLicenseInfo license2 = createIwoLicense("2", LicenseTypeEnum.PREMIER, LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo license3 = createIwoLicense("3", LicenseTypeEnum.PREMIER, LicenseStateEnum.OUTOFCOMPLIANCE);

        List<LicenseLicenseInfo> licenses = Arrays.asList(license2, license3, license1);
        licenses.sort(new BestAvailableIntersightLicenseComparator());
        // they should be in order of: inactive premier, active essential, active premier
        assertEquals("1", licenses.get(0).getMoid());
        assertEquals("2", licenses.get(1).getMoid());
        assertEquals("3", licenses.get(2).getMoid());
    }

    /**
     * Verify that IntersightLicenseUtils.pickBestAvailableLicense() will pick the best license when
     * multiple choices are given.
     */
    @Test
    public void testPickBestAvailableLicense() {
        LicenseLicenseInfo inactivePremier = createIwoLicense("1", LicenseTypeEnum.PREMIER, LicenseStateEnum.GRACEEXPIRED);
        LicenseLicenseInfo activePremier = createIwoLicense("2", LicenseTypeEnum.PREMIER, LicenseStateEnum.COMPLIANCE);
        LicenseLicenseInfo activeEssential = createIwoLicense("3", LicenseTypeEnum.ESSENTIAL, LicenseStateEnum.COMPLIANCE);

        List<LicenseLicenseInfo> licenses = Arrays.asList(activePremier, activeEssential, inactivePremier);
        Optional<LicenseLicenseInfo> optionalBestAvailable = IntersightLicenseUtils.pickBestAvailableLicense(licenses);
        assertTrue(optionalBestAvailable.isPresent());
        assertEquals(activePremier, optionalBestAvailable.get());
    }

    /**
     * Validate that pickBestAvailableLicense will return an empty optional when there are no
     * licenses to choose from.
     */
    @Test
    public void testPickBestAvailableLicenseNoLicenses() {
        assertEquals(Optional.empty(), IntersightLicenseUtils.pickBestAvailableLicense(Collections.emptyList()));
    }
}
