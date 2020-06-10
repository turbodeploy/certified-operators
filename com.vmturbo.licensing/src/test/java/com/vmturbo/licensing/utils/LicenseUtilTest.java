package com.vmturbo.licensing.utils;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.licensing.License;

public class LicenseUtilTest {

    @Test
    public void generateLicenseKey_should_generate_a_hash_of_the_license_details() throws Exception {
        // GIVEN
        String expirationDate = "2050-12-31";
        String email = "gregorytdunn91@gmail.com";
        int numSockets = -1;

        SortedSet<String> cwomEssentialsFeatures = CWOMLicenseEdition.CWOM_ESSENTIALS.getFeatures();
        SortedSet<String> cwomAdvancedFeatures = CWOMLicenseEdition.CWOM_ADVANCED.getFeatures();
        SortedSet<String> cwomPremierFeatures = CWOMLicenseEdition.CWOM_PREMIER.getFeatures();

        //THEN
        assertEquals("db6c59d2fbda1edca789db2040d8d71a", LicenseUtil.generateLicenseKey(expirationDate, email, cwomEssentialsFeatures, numSockets, CountedEntity.VM, null));
        assertEquals("843ee7b1f38435de52ac73db970b25db", LicenseUtil.generateLicenseKey(expirationDate, email, cwomAdvancedFeatures, numSockets, CountedEntity.VM, null));
        assertEquals("8bb961a301f5828b98fc5a2c7e15ded6", LicenseUtil.generateLicenseKey(expirationDate, email, cwomPremierFeatures, numSockets, CountedEntity.VM, null));
    }

    @Test
    public void isValid_should_return_no_errors_for_valid_a_license() throws Exception {
        // GIVEN
        License license = new License();
        license.setEmail("gregorytdunn91@gmail.com");
        license.setExpirationDate("2050-12-31");
        license.setLicenseKey("1bb8c6777a211cd35c90b9c829e5f252");
        license.setNumLicensedEntities(-1);
        license.setCountedEntity(ILicense.CountedEntity.SOCKET);
        license.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));
        // THEN
        // this will fail on 2050-12-31
        assertEquals(0, LicenseUtil.validate(license).size());
    }

    @Test
    public void isValid_should_return_no_errors_for_a_valid_externalLicense_with_a_null_externalLicense_key() throws Exception {
        /* An external license (e.g. CWOM) added to a 6.0.x derived instance, after upgrading to 6.1.x may be perceived as
          an external license, having an external license key that is null and this should not fail validation */
        // GIVEN
        License externalLicense = new License();
        externalLicense.setEmail("gregorytdunn91@gmail.com");
        externalLicense.setExpirationDate("2050-12-31");
        externalLicense.setLicenseKey(null);
        externalLicense.setExternalLicense(true);
        externalLicense.setExternalLicenseKey(null);
        externalLicense.setNumLicensedEntities(-1);
        externalLicense.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));
        externalLicense.setCountedEntity(ILicense.CountedEntity.SOCKET);

        // THEN
        // this will fail on 2050-12-31
        assertEquals(0, LicenseUtil.validate(externalLicense).size());
    }

    @Test
    public void isValid_should_return_invalid_email_if_email_is_missing() throws Exception {
        // GIVEN
        License license = new License();
        license.setEmail("");
        license.setExpirationDate("2050-12-31");
        license.setLicenseKey("4f10b17427920ba29526df392ca1408a");
        license.setNumLicensedEntities(-1);
        license.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));
        // THEN
        List<ErrorReason> issues = Lists.newArrayList(LicenseUtil.validate(license));
        assertEquals(1, issues.size());
        assertEquals(ErrorReason.INVALID_EMAIL, issues.get(0));
    }

    @Test
    public void isValid_should_return_expired_if_license_has_expired() throws Exception {
        // GIVEN
        License license = new License();
        license.setEmail("gregorytdunn91@gmail.com");
        license.setExpirationDate("2001-12-31");
        license.setLicenseKey("b14950148ea696c6747b452308c30bae");
        license.setNumLicensedEntities(-1);
        license.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));

        // THEN
        List<ErrorReason> issues = Lists.newArrayList(LicenseUtil.validate(license));
        assertEquals(1, issues.size());
        assertEquals(ErrorReason.EXPIRED, issues.get(0));

    }

    @Test
    public void isValid_should_return_invalid_license_key_if_license_key_is_invalid() throws Exception {
        // GIVEN
        License license = new License();
        license.setEmail("gregorytdunn91@gmail.com");
        license.setExpirationDate("2050-12-31");
        license.setLicenseKey("abc");
        license.setNumLicensedEntities(-1);
        license.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));
        license.setCountedEntity(ILicense.CountedEntity.SOCKET);

        // THEN
        List<ErrorReason> issues = Lists.newArrayList(LicenseUtil.validate(license));
        assertEquals(1, issues.size());
        assertEquals(ErrorReason.INVALID_LICENSE_KEY, issues.get(0));
    }

    @Test
    public void isValid_should_return_invalid_for_externalLicense_with_an_externalLicenseKey_but_invalid_licenseKey() throws Exception {
        // GIVEN
        License externalLicense = new License();
        externalLicense.setEmail("gregorytdunn91@gmail.com");
        externalLicense.setExpirationDate("2050-12-31");
        externalLicense.setLicenseKey("invalid_key");
        externalLicense.setExternalLicense(true);
        externalLicense.setExternalLicenseKey(":externalLicenseKey");
        externalLicense.setNumLicensedEntities(-1);
        externalLicense.setCountedEntity(ILicense.CountedEntity.SOCKET);
        externalLicense.setFeatures(LicenseFeatureFactory.getFeatureSet(LicenseFeatureFactory.LicenseType.TRIAL));
        // THEN
        List<ErrorReason> issues = Lists.newArrayList(LicenseUtil.validate(externalLicense));
        assertEquals(1, issues.size());
        assertEquals(ErrorReason.INVALID_LICENSE_KEY, issues.get(0));
    }

    @Test
    public void canonicalizeFeatures_should_return_a_list_of_distinct_features_in_alphabetical_order() throws Exception {
        assertEquals("aaadddeeeqqqzzz", LicenseUtil.canonicalizeFeatures(Lists.newArrayList("zzz", "ddd", "aaa", "eee", "aaa", "qqq")));
        assertEquals("", LicenseUtil.canonicalizeFeatures(null));
    }




}

