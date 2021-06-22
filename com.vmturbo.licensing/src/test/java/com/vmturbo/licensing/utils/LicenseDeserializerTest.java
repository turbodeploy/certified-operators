package com.vmturbo.licensing.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TypeCase;
import com.vmturbo.licensing.License;

/**
 * A test case for the {@link LicenseDeserializer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class LicenseDeserializerTest {

    private String cwomLicense1;
    private String cwomLicense2;

    /**
     * Runs before each unit test to initialize fixtures.
     *
     * @throws IOException If the license fails to be read from disk or be parsed.
     */
    @Before
    public void setUp() throws IOException {
        cwomLicense1 = IOUtils.toString(
            getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_1.lic"),
            StandardCharsets.UTF_8);
        cwomLicense2 = IOUtils.toString(
            getClass().getResourceAsStream("LicenseDeserializationTest_cwom_premier_license_2.lic"),
            StandardCharsets.UTF_8);

    }

    /**
     * Test that a Grafana license gets deserialized into an {@link ExternalLicense} properly.
     *
     * @throws IOException If the license fails to be read from disk or be parsed.
     */
    @Test
    public void testGrafanaLicense() throws IOException {
        String grafanaLicense = IOUtils.toString(
            getClass().getResourceAsStream("grafana.jwt"),
            StandardCharsets.UTF_8);
        LicenseDTO license = LicenseDeserializer.deserialize(grafanaLicense, "grafana.jwt");
        assertThat(license.getTypeCase(), is(TypeCase.EXTERNAL));

        ExternalLicense externalLicense = license.getExternal();
        DecodedJWT decoded = JWT.decode(grafanaLicense);
        assertThat(externalLicense.getExpirationDate(),
                is(DateTimeUtil.formatDate(decoded.getExpiresAt())));
        assertThat(externalLicense.getPayload(), is(grafanaLicense));
        assertThat(license.getFilename(), is("grafana.jwt"));
    }

    /**
     * Tests license key is extracted correctly from the license and that validation fails for
     * expired licenses.
     */
    @Test
    public void deserializeShouldCreateALicenseApiInputDTOFromCwomLicense() {
        LicenseDTO license = LicenseDeserializer.deserialize(cwomLicense1, "license.lic");
        assertThat(license.getTypeCase(), is(TypeCase.TURBO));

        ILicense model = LicenseUtil.toModel(license).get();
        assertEquals("0f3d667ce289b60e22f40d7835f8fe6b", LicenseUtil.generateLicenseKey(model));

        assertEquals(1, LicenseUtil.validate(model).size());
        assertEquals(ErrorReason.EXPIRED, Iterables.get(LicenseUtil.validate(model), 0));
    }

    /**
     * Tests that similar but not identical CWOM licenses get different license keys.
     */
    @Test
    public void deserializeShouldGenerateDifferentTurboLicenseKeysForSimilarCwomLicenses() {
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
        assertEquals(turboLicense1.getNumLicensedEntities(),
                    turboLicense2.getNumLicensedEntities());
        assertEquals(turboLicense1.getCountedEntity(), turboLicense2.getCountedEntity());
        assertTrue(LicenseUtil.equalFeatures(turboLicense1.getFeaturesList(),
                                            turboLicense2.getFeaturesList()));
        assertNotEquals(turboLicense1.getExternalLicenseKey(),
                        turboLicense2.getExternalLicenseKey());

        // THEN
        assertNotEquals(turboLicense1.getLicenseKey(), turboLicense2.getLicenseKey());
    }

    /**
     * Tests that deserialization returns {@link ErrorReason#INVALID_CONTENT_TYPE} error for
     * licenses with invalid content.
     */
    @Test
    public void deserializeShouldReturnALicenseWithInvalidContextTypeWhenBogusContent() {
        LicenseDTO licenseApiDTO = LicenseDeserializer.deserialize("foo-bar", "license1.lic");
        assertEquals(ErrorReason.INVALID_CONTENT_TYPE.name(),
                    Iterables.get(licenseApiDTO.getTurbo().getErrorReasonList(), 0));
    }

    /**
     * Tests that loading a Turbonomic XML license from file produces the expected Java object.
     *
     * @param licenseFileName The name of the XML license file to load.
     * @param expectedLicense A map providing the expected return value for each getter method after
     *                        the license is loaded.
     *
     * @throws IOException If the license fails to be read from disk or be parsed.
     * @throws InvocationTargetException If one of the accessor methods of
     *     {@link LicenseDTO.TurboLicense} throws an exception. That would signal a problem with the
     *     corresponding accessor.
     * @throws IllegalAccessException Should never be thrown unless there is a mistake in the test
     *     case itself.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Contents of {0} are deserialized as {1}.")
    public void testLoadingXmlLicenseFromFile(String licenseFileName,
                                                                Map<String, String> expectedLicense)
                            throws IOException, InvocationTargetException, IllegalAccessException {
        String licenseFileContents = IOUtils.toString(
            getClass().getResourceAsStream(licenseFileName),
            StandardCharsets.UTF_8);
        LicenseDTO actualLicense = LicenseDeserializer.deserialize(licenseFileContents,
                                                                    licenseFileName);

        assertEquals(licenseFileName, actualLicense.getFilename());
        assertEquals(TypeCase.TURBO, actualLicense.getTypeCase());
        assertTrue(actualLicense.hasTurbo());

        for (Method method : LicenseDTO.TurboLicense.class.getDeclaredMethods()) {
            // Only test public, non-deprecated getters excluding ones always added by Protobuf.
            if (method.getName().startsWith("get")
                && !method.getName().matches(".*(Bytes|List|Descriptor|Instance|ForType"
                                                + "|Fields|SerializedSize)$")
                && method.getAnnotation(Deprecated.class) == null
                && method.getParameterCount() == 0
                && Modifier.isPublic(method.getModifiers())) {
                // Checking that the method is in the map will ensure people that add new getters
                // will be reminded to update these test cases to cover them.
                assertTrue(expectedLicense.containsKey(method.getName()));
                assertEquals(expectedLicense.get(method.getName()),
                    method.invoke(actualLicense.getTurbo(), (Object[])null));
            }
        }
    }

    /**
     * Generates test input for the corresponding test case.
     *
     * @return The test input.
     */
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestLoadingXmlLicenseFromFile() {
        TreeSet<String> commonFeatures = new TreeSet<>(Arrays.asList(
            "historical_data",
            "custom_reports",
            "planner",
            "optimizer",
            "multiple_vc",
            "scoped_user_view",
            "customized_views",
            "group_editor",
            "vmturbo_api",
            "automated_actions",
            "active_directory",
            "full_policy",
            "applications",
            "app_control",
            "loadbalancer",
            "deploy",
            "aggregation",
            "fabric",
            "storage",
            "cloud_targets",
            "cluster_flattening",
            "network_control",
            "container_control",
            "public_cloud"
        ));

        // Creating anonymous subclass of HashMap because ImmutableMap.of can't accept null values
        // or more than 5 entries.
        return new Object[][]{
            {"turbonomic-license-with-customer_id.xml", new ImmutableMap.Builder<>()
                .put("getLicenseOwner", "mitchell Lau")
                .put("getEmail", "mitchell.lau@turbonomic.com")
                .put("getCustomerId", "0010b00002KwSWXAA3")
                .put("getNumLicensedEntities", 10000)
                .put("getEdition", "Trial")
                .put("getExpirationDate", "2022-04-13")
                .put("getLicenseKey", "a1af0dfd21acd5682b29a64ab349a1a8")
                .put("getCountedEntity", CountedEntity.VM.name())
                .put("getIsValid", false)
                .put("getExternalLicenseKey", "")
                .put("getFeaturesCount", 29)
                .put("getErrorReasonCount", 0)
                .put("getFeatures", new TreeSet<>(Sets.union(
                    commonFeatures,
                    new TreeSet<>(Arrays.asList(
                        "vdi_control",
                        "scaling",
                        "custom_policies",
                        "action_script",
                        "SLA"
                    )))))
                .build()
            },
            // Verify that the counted entity type is set to "VM" even if vm-total is set to zero.
            {"LicenseDeserializationTest_license_0vm.xml", new ImmutableMap.Builder<>()
                .put("getLicenseOwner", "Samwell Rockwise")
                .put("getEmail", "samwell.rockwise@turbonomic.com")
                .put("getCustomerId", LicenseDeserializer.CUSTOMER_ID_MISSING)
                .put("getNumLicensedEntities", 0)
                .put("getEdition", "")
                .put("getExpirationDate", "2050-12-31")
                .put("getLicenseKey", "c8a2f34d660caa921fa83b2836f0ce15")
                .put("getCountedEntity", CountedEntity.VM.name())
                .put("getIsValid", false)
                .put("getExternalLicenseKey", "")
                .put("getFeaturesCount", 1)
                .put("getErrorReasonCount", 0)
                .put("getFeatures", Collections.singletonList("easter-egg"))
                .build()
            },
            {"LicenseDeserializationTest_license_v1.xml", new ImmutableMap.Builder<>()
                .put("getLicenseOwner", "Saipriya Balasubramanian")
                .put("getEmail", "saipriya.balasubramanian@turbonomic.com")
                .put("getCustomerId", LicenseDeserializer.CUSTOMER_ID_MISSING)
                .put("getNumLicensedEntities", 200)
                .put("getEdition", "")
                .put("getExpirationDate", "2050-01-31")
                .put("getLicenseKey", "1944c723f8bcaf1ed6831a4f9d865794")
                .put("getCountedEntity", CountedEntity.SOCKET.name())
                .put("getIsValid", false)
                .put("getExternalLicenseKey", "")
                .put("getFeaturesCount", 25)
                .put("getErrorReasonCount", 0)
                .put("getFeatures", new TreeSet<>(Sets.union(
                    commonFeatures,
                    new TreeSet<>(Collections.singletonList("cloud_cost")))))
                .build()
            },
            {"LicenseDeserializationTest_license_v2.xml", new ImmutableMap.Builder<>()
                .put("getLicenseOwner", "Giampiero De Ciantis")
                .put("getEmail", "gp.deciantis@turbonomic.com")
                .put("getCustomerId", LicenseDeserializer.CUSTOMER_ID_MISSING)
                .put("getNumLicensedEntities", 10000)
                .put("getEdition", "Turbonomic Best")
                .put("getExpirationDate", "2022-08-18")
                .put("getLicenseKey", "92001124fe07e4f7d0f24cb27bf7c215")
                .put("getCountedEntity", CountedEntity.VM.name())
                .put("getIsValid", false)
                .put("getExternalLicenseKey", "")
                .put("getFeaturesCount", 30)
                .put("getErrorReasonCount", 0)
                .put("getFeatures", new TreeSet<>(Sets.union(
                    commonFeatures,
                    new TreeSet<>(Arrays.asList(
                        "API2",
                        "vdi_control",
                        "scaling",
                        "custom_policies",
                        "action_script",
                        "SLA"
                    )))))
                .build()
            },
            // CWOM licenses are also presented as Turbonomic ones.
            {"LicenseDeserializationTest_cwom_premier_license_1.lic", new ImmutableMap.Builder<>()
                .put("getLicenseOwner", "cisco")
                .put("getEmail", "support@cisco.com")
                .put("getCustomerId", "missing from license DTO")
                .put("getNumLicensedEntities", 1000)
                .put("getEdition", "CWOM_PREMIER")
                .put("getExpirationDate", "2018-02-07")
                .put("getLicenseKey", "0f3d667ce289b60e22f40d7835f8fe6b")
                .put("getCountedEntity", CountedEntity.VM.name())
                .put("getIsValid", false)
                .put("getExternalLicenseKey", "1574 51BC 4532 582A C2EC 505B 4A2C 7920 8F1D 3BBD "
                                            + "ADE8 34C3 1E90 5E52 C1EF 0778 F120 8312 D27B 7182 "
                                            + "A54D 8756 5CCF 5FEF 9FA7 5891 5AA6 868A A61D 3B71")
                .put("getFeaturesCount", 31)
                .put("getErrorReasonCount", 0)
                .put("getFeatures", String.join(",", CWOMLicenseEdition.CWOM_PREMIER.getFeatures()))
                .build()
            },
        };
    }

    /**
     * Tests that license classification functions return the expected results.
     *
     * <p>The functions tested are {@link LicenseDeserializer#isWellFormedXML(String)},
     * {@link LicenseDeserializer#isLicenseGeneratedByFlexlm(String)} and
     * {@link LicenseDeserializer#isGrafanaJwt(String)}.</p>
     *
     * @param licenseFileName The file name of the license to test.
     * @param expectedXMLResult The value <b>isWellFormedXML</b> is expected to return for the given
     *                          license.
     * @param expectedFlexlmResult The value <b>isLicenseGeneratedByFlexlm</b> is expected to return
     *                             for the given license.
     * @param expectedGrafanaResult The value <b>isGrafanaJwt</b> is expected to return for the
     *                              given license.
     * @throws IOException If the license fails to load from disk.
     */
    @Test
    @Parameters({
        "LicenseDeserializationTest_license_0vm.xml, true, false, false",
        "LicenseDeserializationTest_license_v1.xml, true, false, false",
        "LicenseDeserializationTest_license_v2.xml, true, false, false",
        "turbonomic-license-with-customer_id.xml, true, false, false",
        "LicenseDeserializationTest_cwom_premier_license_1.lic, false, true, false",
        "LicenseDeserializationTest_cwom_premier_license_2.lic, false, true, false",
        "LicenseUtilTest_CWOMExpired.lic, false, true, false",
        "LicenseUtilTest_CWOMGood.lic, false, true, false",
        "grafana.jwt, false, false, true",
    })
    @TestCaseName("Test #{index}: isWellFormedXML({0}) == {1} "
                + "and isLicenseGeneratedByFlexlm({0}) == {2} and isGrafanaJwt({0}) == {3}")
    public void testLicenseClassificationFunctions(String licenseFileName,
            boolean expectedXMLResult, boolean expectedFlexlmResult, boolean expectedGrafanaResult)
                                                                                throws IOException {
        String licenseContents = IOUtils.toString(
            getClass().getResourceAsStream(licenseFileName),
            StandardCharsets.UTF_8);

        assertEquals(expectedXMLResult, LicenseDeserializer.isWellFormedXML(licenseContents));
        assertEquals(expectedFlexlmResult,
                                LicenseDeserializer.isLicenseGeneratedByFlexlm(licenseContents));
        assertEquals(expectedGrafanaResult, LicenseDeserializer.isGrafanaJwt(licenseContents));
    }

    /**
     * Verify XXE attack is prevented by disabling XML external entity and DTD processing.
     *
     * <p>This is a negative, security-related test.</p>
     *
     * @param licenseFileName The file name of the license to parse.
     * @throws IOException If the license fails to load from disk.
     */
    @Test(expected = SecurityException.class)
    @Parameters({
        "LicenseDeserializationTest_XXE_attack_1.xml",
        "LicenseDeserializationTest_XXE_attack_2.xml",
        "LicenseDeserializationTest_XXE_attack_3.xml",
    })
    @TestCaseName("Test #{index}: isWellFormedXML({0}) throws SecurityException")
    public void testIsWellFormedXmlPreventsXxeAttacks(String licenseFileName) throws IOException {
        String licenseContents = IOUtils.toString(
            getClass().getResourceAsStream(licenseFileName),
            StandardCharsets.UTF_8);

        LicenseDeserializer.isWellFormedXML(licenseContents);
    }

    /**
     * Test that licenses are validated properly.
     *
     * @param licenseFileName The file name of the license to validate.
     * @param nExpectedErrors The number of errors we expect validation to return.
     * @throws IOException If the license fails to load from disk.
     */
    @Test
    @Parameters({
        "LicenseDeserializationTest_license_v1.xml, 0",
        "LicenseDeserializationTest_license_v2.xml, 0",
        "LicenseDeserializationTest_cwom_premier_license_1.lic, 1",
    })
    @TestCaseName("Test #{index}: LicenseUtil.validate({0}) returns {1} error(s).")
    public void testLicenseValidation(String licenseFileName, int nExpectedErrors)
                                                                                throws IOException {
        String licenseFileContents = IOUtils.toString(
            getClass().getResourceAsStream(licenseFileName),
            StandardCharsets.UTF_8);
        LicenseDTO license = LicenseDeserializer.deserialize(licenseFileContents, licenseFileName);
        Optional<License> licenseModel = LicenseUtil.toModel(license);

        assertTrue(licenseModel.isPresent());
        assertEquals(nExpectedErrors, LicenseUtil.validate(licenseModel.get()).size());
    }

} // End LicenseDeserializerTest
