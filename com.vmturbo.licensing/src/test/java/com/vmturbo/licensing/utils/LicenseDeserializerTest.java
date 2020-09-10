package com.vmturbo.licensing.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.collect.Iterables;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TypeCase;

public class LicenseDeserializerTest {

    private String turboLicenseV1;
    private String turboLicenseV2;
    private String cwomLicense1;
    private String cwomLicense2;
    private String grafanaLicense;

    @Before
    public void setUp() throws Exception {
        turboLicenseV1 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_license_v1.xml"), "UTF-8");
        turboLicenseV2 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_license_v2.xml"), "UTF-8");
        cwomLicense1 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_1.lic"), "UTF-8");
        cwomLicense2 = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_2.lic"), "UTF-8");
        grafanaLicense = IOUtils.toString(getClass().getResourceAsStream("grafana.jwt"), "UTF-8");
    }

    /**
     * Test that a Grafana license gets deserialized into an {@link ExternalLicense} properly.
     */
    @Test
    public void testGrafanaLicense() {
        LicenseDTO license = LicenseDeserializer.deserialize(grafanaLicense, "grafana.jwt");
        assertThat(license.getTypeCase(), is(TypeCase.EXTERNAL));

        ExternalLicense externalLicense = license.getExternal();
        DecodedJWT decoded = JWT.decode(grafanaLicense);
        assertThat(externalLicense.getExpirationDate(), is(DateTimeUtil.formatDate(decoded.getExpiresAt())));
        assertThat(externalLicense.getPayload(), is(grafanaLicense));
        assertThat(license.getFilename(), is("grafana.jwt"));
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_turbo_license_v1_XML() throws Exception {
        // WHEN
        LicenseDTO license = LicenseDeserializer.deserialize(turboLicenseV1, "license.xml");
        assertThat(license.getTypeCase(), is(TypeCase.TURBO));

        TurboLicense turboLicense = license.getTurbo();

        // THEN
        assertEquals("Saipriya Balasubramanian", turboLicense.getLicenseOwner());
        assertEquals("saipriya.balasubramanian@turbonomic.com", turboLicense.getEmail());
        assertFalse(turboLicense.hasExternalLicenseKey());
        assertEquals(ILicense.CountedEntity.SOCKET.name(), turboLicense.getCountedEntity());
        assertEquals(200, turboLicense.getNumLicensedEntities());
        assertEquals("2050-01-31", turboLicense.getExpirationDate());
        assertFalse(turboLicense.hasEdition());
        assertEquals("license.xml", license.getFilename());
        assertEquals("1944c723f8bcaf1ed6831a4f9d865794", turboLicense.getLicenseKey());
        assertEquals(25, turboLicense.getFeaturesList().size());

        String features = turboLicense.getFeaturesList().stream().collect(Collectors.joining(","));
        assertEquals("active_directory,aggregation,app_control,applications,automated_actions,cloud_cost,cloud_targets,cluster_flattening,container_control,custom_reports,customized_views,deploy,fabric,full_policy,group_editor,historical_data,loadbalancer,multiple_vc,network_control,optimizer,planner,public_cloud,scoped_user_view,storage,vmturbo_api", features);

        // this will fail on 2050-01-31
        assertEquals(0, LicenseUtil.validate(LicenseUtil.toModel(license).get()).size());
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_turbo_license_v2_XML() throws Exception {
        // WHEN
        LicenseDTO license = LicenseDeserializer.deserialize(turboLicenseV2, "license.xml");
        assertThat(license.getTypeCase(), is(TypeCase.TURBO));

        TurboLicense turboLicense = license.getTurbo();

        // THEN
        assertEquals("Giampiero De Ciantis", turboLicense.getLicenseOwner());
        assertEquals("gp.deciantis@turbonomic.com", turboLicense.getEmail());
        assertFalse(turboLicense.hasExternalLicenseKey());
        assertEquals(ILicense.CountedEntity.VM.name(), turboLicense.getCountedEntity());
        assertEquals(10000, turboLicense.getNumLicensedEntities());
        assertEquals("2022-08-18", turboLicense.getExpirationDate());
        assertEquals("Turbonomic Best", turboLicense.getEdition());
        assertEquals("license.xml", license.getFilename());
        assertEquals("92001124fe07e4f7d0f24cb27bf7c215", turboLicense.getLicenseKey());
        assertEquals(30, turboLicense.getFeaturesList().size());

        String features = turboLicense.getFeaturesList().stream().collect(Collectors.joining(","));
        assertEquals("API2,SLA,action_script,active_directory,aggregation,app_control,applications,automated_actions,cloud_targets,cluster_flattening,container_control,custom_policies,custom_reports,customized_views,deploy,fabric,full_policy,group_editor,historical_data,loadbalancer,multiple_vc,network_control,optimizer,planner,public_cloud,scaling,scoped_user_view,storage,vdi_control,vmturbo_api", features);

        // this will fail on 2022-08-18
        assertEquals(0, LicenseUtil.validate(LicenseUtil.toModel(license).get()).size());
    }

    @Test
    public void deserialize_should_create_a_LicenseApiInputDTO_from_cwom_license() throws Exception {
        // WHEN
        LicenseDTO license = LicenseDeserializer.deserialize(cwomLicense1, "license.lic");
        assertThat(license.getTypeCase(), is(TypeCase.TURBO));

        TurboLicense turboLicense = license.getTurbo();

        // THEN
        assertEquals("cisco", turboLicense.getLicenseOwner());
        assertEquals("support@cisco.com", turboLicense.getEmail());
        assertTrue(turboLicense.hasExternalLicenseKey());
        assertEquals(ILicense.CountedEntity.VM.name(), turboLicense.getCountedEntity());
        assertEquals(1000, turboLicense.getNumLicensedEntities());
        assertEquals("2018-02-07", turboLicense.getExpirationDate());
        assertEquals("CWOM_PREMIER", turboLicense.getEdition());
        assertEquals("license.lic", license.getFilename());
        assertEquals("1574 51BC 4532 582A C2EC 505B 4A2C 7920 8F1D 3BBD ADE8 34C3 1E90 5E52 C1EF 0778 F120 8312 D27B 7182 A54D 8756 5CCF 5FEF 9FA7 5891 5AA6 868A A61D 3B71", turboLicense.getExternalLicenseKey());
        assertEquals("0f3d667ce289b60e22f40d7835f8fe6b", turboLicense.getLicenseKey());
        assertEquals(31, turboLicense.getFeaturesList().size());

        String features = turboLicense.getFeaturesList().stream().collect(Collectors.joining(","));
        String expectedFeatures = CWOMLicenseEdition.CWOM_PREMIER.getFeatures().stream().collect(Collectors.joining(","));
        assertEquals(expectedFeatures, features);

        ILicense model = LicenseUtil.toModel(license).get();
        assertEquals("0f3d667ce289b60e22f40d7835f8fe6b", LicenseUtil.generateLicenseKey(model));

        assertEquals(1, LicenseUtil.validate(model).size());
        assertEquals(ErrorReason.EXPIRED, Iterables.get(LicenseUtil.validate(model), 0));
    }

    @Test
    public void deserialize_should_generate_different_turbo_license_keys_for_similar_cwom_licenses() throws Exception {
        // GIVEN
        LicenseDTO license1 = LicenseDeserializer.deserialize(cwomLicense1, "license1.lic");
        LicenseDTO license2 = LicenseDeserializer.deserialize(cwomLicense2, "license2.lic");
        assertEquals(license1.getTypeCase(), license2.getTypeCase());
        assertThat(license1.getTypeCase(), is(TypeCase.TURBO));

        TurboLicense turboLicense1 = license1.getTurbo();
        TurboLicense turboLicense2 = license2.getTurbo();


        // WHEN
        assertEquals(turboLicense1.getExpirationDate(), turboLicense2.getExpirationDate());
        assertEquals(turboLicense1.getEmail(), turboLicense2.getEmail());
        assertEquals(turboLicense1.getNumLicensedEntities(), turboLicense2.getNumLicensedEntities());
        assertEquals(turboLicense1.getCountedEntity(), turboLicense2.getCountedEntity());
        assertTrue(LicenseUtil.equalFeatures(turboLicense1.getFeaturesList(), turboLicense2.getFeaturesList()));
        assertNotEquals(turboLicense1.getExternalLicenseKey(), turboLicense2.getExternalLicenseKey());

        // THEN
        assertNotEquals(turboLicense1.getLicenseKey(), turboLicense2.getLicenseKey());
    }

    @Test
    public void deserialize_should_return_a_license_with_inavlid_context_type_when_bogus_content() throws Exception {
        LicenseDTO licenseApiDTO = LicenseDeserializer.deserialize("foo-bar", "license1.lic");
        assertEquals(ErrorReason.INVALID_CONTENT_TYPE.name(), Iterables.get(licenseApiDTO.getTurbo().getErrorReasonList(), 0));
    }

    /**
     * Verify that the counted entity type is set to "VM" even if vm-total is set to zero.
     * @throws IOException
     */
    @Test
    public void testDeserializeXmlLicenseWith0VM() throws IOException {
        String licenseXMLWith0VMTotal = IOUtils.toString(getClass().getResourceAsStream("LicenseDeserializationTest_license_0vm.xml"), "UTF-8");
        LicenseDTO license = LicenseDeserializer.deserialize(licenseXMLWith0VMTotal, "license.xml");
        assertTrue(license.hasTurbo());
        assertEquals(CountedEntity.VM.name(), license.getTurbo().getCountedEntity());
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

    /**
     * Verify XXE attack is prevented by disabling XML external entity and DTD processing.
     *
     * @throws IOException if fails to parse license.
     */
    @Test(expected = SecurityException.class)
    public void isWellFormedXmlXxeAttack() throws IOException {
        String turboLicenseXXE = IOUtils.toString(
                getClass().getResourceAsStream("LicenseDeserializationTest_XXE_attack_1.xml"),
                StandardCharsets.UTF_8);
        String turboLicenseXXE2 = IOUtils.toString(
                getClass().getResourceAsStream("LicenseDeserializationTest_XXE_attack_2.xml"),
                StandardCharsets.UTF_8);
        String turboLicenseXXE3 = IOUtils.toString(
                getClass().getResourceAsStream("LicenseDeserializationTest_XXE_attack_3.xml"),
                StandardCharsets.UTF_8);

        try {
            LicenseDeserializer.isWellFormedXML(turboLicenseXXE);
            fail("Security exception should throw.");
        } catch (SecurityException e) {
            //expected
        }
        try {
            LicenseDeserializer.isWellFormedXML(turboLicenseXXE2);
            fail("Security exception should throw.");
        } catch (SecurityException e) {
            //expected
        }
        LicenseDeserializer.isWellFormedXML(turboLicenseXXE3);
        fail("Security exception should throw.");
    }

}