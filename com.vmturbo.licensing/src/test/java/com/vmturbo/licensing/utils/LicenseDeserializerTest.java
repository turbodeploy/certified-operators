package com.vmturbo.licensing.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.dto.license.LicenseApiDTO;

public class LicenseDeserializerTest {

    private String turboLicenseV1;
    private String turboLicenseV2;
    private String cwomLicense1;
    private String cwomLicense2;

    @Before
    public void setUp() throws Exception {
        turboLicenseV1 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_license_v1.xml"), "UTF-8");
        turboLicenseV2 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_license_v2.xml"), "UTF-8");
        cwomLicense1 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_1.lic"), "UTF-8");
        cwomLicense2 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_2.lic"), "UTF-8");
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_turbo_license_v1_XML() throws Exception {
        // WHEN
        LicenseApiDTO licenseApiDTO = LicenseDeserializer.deserialize(turboLicenseV1, "license.xml");

        // THEN
        assertEquals("Saipriya Balasubramanian", licenseApiDTO.getLicenseOwner());
        assertEquals("saipriya.balasubramanian@turbonomic.com", licenseApiDTO.getEmail());
        assertFalse(licenseApiDTO.isExternalLicense());
        assertEquals(ILicense.CountedEntity.SOCKET, licenseApiDTO.getCountedEntity());
        assertEquals(200, licenseApiDTO.getNumLicensedEntities());
        assertEquals("2050-01-31", licenseApiDTO.getExpirationDate());
        assertNull(licenseApiDTO.getEdition());
        assertEquals("license.xml", licenseApiDTO.getFilename());
        assertEquals("1944c723f8bcaf1ed6831a4f9d865794", licenseApiDTO.getLicenseKey());
        assertEquals(25, licenseApiDTO.getFeatures().size());

        String features = licenseApiDTO.getFeatures().stream().collect(Collectors.joining(","));
        assertEquals("active_directory,aggregation,app_control,applications,automated_actions,cloud_cost,cloud_targets,cluster_flattening,container_control,custom_reports,customized_views,deploy,fabric,full_policy,group_editor,historical_data,loadbalancer,multiple_vc,network_control,optimizer,planner,public_cloud,scoped_user_view,storage,vmturbo_api", features);

        // this will fail on 2050-01-31
        assertEquals(0, LicenseUtil.validate(licenseApiDTO).size());
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_turbo_license_v2_XML() throws Exception {
        // WHEN
        LicenseApiDTO licenseApiDTO = LicenseDeserializer.deserialize(turboLicenseV2, "license.xml");

        // THEN
        assertEquals("Giampiero De Ciantis", licenseApiDTO.getLicenseOwner());
        assertEquals("gp.deciantis@turbonomic.com", licenseApiDTO.getEmail());
        assertFalse(licenseApiDTO.isExternalLicense());
        assertEquals(ILicense.CountedEntity.VM, licenseApiDTO.getCountedEntity());
        assertEquals(10000, licenseApiDTO.getNumLicensedEntities());
        assertEquals("2022-08-18", licenseApiDTO.getExpirationDate());
        assertEquals("Turbonomic Best", licenseApiDTO.getEdition());
        assertEquals("license.xml", licenseApiDTO.getFilename());
        assertEquals("92001124fe07e4f7d0f24cb27bf7c215", licenseApiDTO.getLicenseKey());
        assertEquals(30, licenseApiDTO.getFeatures().size());

        String features = licenseApiDTO.getFeatures().stream().collect(Collectors.joining(","));
        assertEquals("API2,SLA,action_script,active_directory,aggregation,app_control,applications,automated_actions,cloud_targets,cluster_flattening,container_control,custom_policies,custom_reports,customized_views,deploy,fabric,full_policy,group_editor,historical_data,loadbalancer,multiple_vc,network_control,optimizer,planner,public_cloud,scaling,scoped_user_view,storage,vdi_control,vmturbo_api", features);

        // this will fail on 2022-08-18
        assertEquals(0, LicenseUtil.validate(licenseApiDTO).size());
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_cwom_license() throws Exception {
        // WHEN
        LicenseApiDTO licenseApiDTO = LicenseDeserializer.deserialize(cwomLicense1, "license.lic");

        // THEN
        assertEquals("cisco", licenseApiDTO.getLicenseOwner());
        assertEquals("support@cisco.com", licenseApiDTO.getEmail());
        assertTrue(licenseApiDTO.isExternalLicense());
        assertEquals(ILicense.CountedEntity.VM, licenseApiDTO.getCountedEntity());
        assertEquals(1000, licenseApiDTO.getNumLicensedEntities());
        assertEquals("2018-02-07", licenseApiDTO.getExpirationDate());
        assertEquals("CWOM_PREMIER", licenseApiDTO.getEdition());
        assertEquals("license.lic", licenseApiDTO.getFilename());
        assertEquals("1574 51BC 4532 582A C2EC 505B 4A2C 7920 8F1D 3BBD ADE8 34C3 1E90 5E52 C1EF 0778 F120 8312 D27B 7182 A54D 8756 5CCF 5FEF 9FA7 5891 5AA6 868A A61D 3B71", licenseApiDTO.getExternalLicenseKey());
        assertEquals("0f3d667ce289b60e22f40d7835f8fe6b", licenseApiDTO.getLicenseKey());
        assertEquals(31, licenseApiDTO.getFeatures().size());

        String features = licenseApiDTO.getFeatures().stream().collect(Collectors.joining(","));
        String expectedFeatures = CWOMLicenseEdition.CWOM_PREMIER.getFeatures().stream().collect(Collectors.joining(","));
        assertEquals(expectedFeatures, features);

        assertEquals("0f3d667ce289b60e22f40d7835f8fe6b", LicenseUtil.generateLicenseKey(licenseApiDTO));

        assertEquals(1, LicenseUtil.validate(licenseApiDTO).size());
        assertEquals(ErrorReason.EXPIRED, Iterables.get(LicenseUtil.validate(licenseApiDTO), 0));
    }

    @Test
    public void deserialize_should_generate_different_turbo_license_keys_for_similar_cwom_licenses() throws Exception {
        // GIVEN
        LicenseApiDTO licenseApiDTO1 = LicenseDeserializer.deserialize(cwomLicense1, "license1.lic");
        LicenseApiDTO licenseApiDTO2 = LicenseDeserializer.deserialize(cwomLicense2, "license2.lic");

        // WHEN
        assertEquals(licenseApiDTO1.getExpirationDate(), licenseApiDTO2.getExpirationDate());
        assertEquals(licenseApiDTO1.getEmail(), licenseApiDTO2.getEmail());
        assertEquals(licenseApiDTO1.getNumLicensedEntities(), licenseApiDTO2.getNumLicensedEntities());
        assertEquals(licenseApiDTO1.getCountedEntity(), licenseApiDTO2.getCountedEntity());
        assertTrue(LicenseUtil.equalFeatures(licenseApiDTO1.getFeatures(), licenseApiDTO2.getFeatures()));
        assertNotEquals(licenseApiDTO1.getExternalLicenseKey(), licenseApiDTO2.getExternalLicenseKey());

        // THEN
        assertNotEquals(licenseApiDTO1.getLicenseKey(), licenseApiDTO2.getLicenseKey());
    }

    @Test
    public void deserialize_should_return_a_license_with_inavlid_context_type_when_bogus_content() throws Exception {
        LicenseApiDTO licenseApiDTO = LicenseDeserializer.deserialize("foo-bar", "license1.lic");
        assertEquals(ErrorReason.INVALID_CONTENT_TYPE, Iterables.get(licenseApiDTO.getErrorReasons(), 0));
    }

    @Test
    public void isWellFormedXML_should_return_false_if_the_text_is_not_well_formed_xml() {
        Assert.assertFalse(LicenseDeserializer.isWellFormedXML(cwomLicense1));
    }

    @Test
    public void isWellFormedXML_should_return_true_if_the_text_is_well_formed_xml()  {
        assertTrue(LicenseDeserializer.isWellFormedXML(turboLicenseV1));
        assertTrue(LicenseDeserializer.isWellFormedXML(turboLicenseV2));
    }

    @Test
    public void isLicenseGeneratedByFlexlm_should_return_true_if_the_text_is_cwom_license() {
        assertTrue(LicenseDeserializer.isLicenseGeneratedByFlexlm(cwomLicense1));
    }

    @Test
    public void isLicenseGeneratedByFlexlm_should_return_false_if_the_text_xml() {
        assertFalse(LicenseDeserializer.isLicenseGeneratedByFlexlm(turboLicenseV1));
        assertFalse(LicenseDeserializer.isLicenseGeneratedByFlexlm(turboLicenseV2));
    }
}