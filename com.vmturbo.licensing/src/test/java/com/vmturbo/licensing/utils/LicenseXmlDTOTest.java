package com.vmturbo.licensing.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.Sets;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.api.dto.license.ILicense.CountedEntity;

/**
 * A test case for the {@link LicenseXmlDTO} class.
 */
@RunWith(JUnitParamsRunner.class)
public class LicenseXmlDTOTest {
    // Methods

    /**
     * Tests that loading a Turbonomic XML license from file produces the expected Java object.
     *
     * @param inputLicenseFile The name of the XML license file to load.
     * @param expectedLicense A map providing the expected return value for each getter method after
     *                        the license is loaded.
     *
     * @throws IOException If the license fails to be read from disk or be parsed.
     * @throws InvocationTargetException If one of the accessor methods of {@link LicenseXmlDTO}
     *     throws an exception. That would signal a problem with the corresponding accessor.
     * @throws IllegalAccessException Should never be thrown unless there is a mistake in the test
     *     case itself.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Contents of {0} are deserialized as {1}.")
    public void testLoadingLicenseFromFile(String inputLicenseFile,
                                           Map<String, String> expectedLicense)
                throws IOException, InvocationTargetException, IllegalAccessException {
        // XML mapper isn't configured to ignore unknown fields so we are reminded to update code to
        // support new ones.
        LicenseXmlDTO actualLicense = new XmlMapper().readValue(
            getClass().getResource(inputLicenseFile),
            LicenseXmlDTO.class);

        for (Method method : LicenseXmlDTO.class.getDeclaredMethods()) {
            // Only test public getters.
            if (method.getName().startsWith("get")
                    // getFeatureNodes will be tested indirectly through getFeatures for simplicity.
                    && !method.getName().equals("getFeatureNodes")
                    && method.getParameterCount() == 0
                    && Modifier.isPublic(method.getModifiers())) {
                // Checking that the method is in the map will ensure people that add new getters
                // will be reminded to update these test cases to cover them.
                assertTrue(expectedLicense.containsKey(method.getName()));
                assertEquals(expectedLicense.get(method.getName()),
                            method.invoke(actualLicense, (Object[])null));
            }
        }
    }

    /**
     * Generates test input for the corresponding test case.
     *
     * @return The test input.
     */
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestLoadingLicenseFromFile() {
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
            {"turbonomic-license-with-customer_id.xml",
                new HashMap<String, Object>() {{
                    put("getFirstName", "mitchell");
                    put("getLastName", "Lau");
                    put("getEmail", "mitchell.lau@turbonomic.com");
                    put("getCustomerId", "0010b00002KwSWXAA3");
                    put("getNumSockets", 0);
                    put("getVmTotal", 10000);
                    put("getNumEntities", 10000);
                    put("getEdition", "Trial");
                    put("getExpirationDate", "2022-04-13");
                    put("getLockCode", "a1af0dfd21acd5682b29a64ab349a1a8");
                    put("getCountedEntity", CountedEntity.VM);
                    put("getFeatures", new TreeSet<>(Sets.union(
                        commonFeatures,
                        new TreeSet<>(Arrays.asList(
                            "vdi_control",
                            "scaling",
                            "custom_policies",
                            "action_script",
                            "SLA"
                        )))));
                }}
            },
            {"LicenseDeserializationTest_license_v1.xml",
                new HashMap<String, Object>() {{
                    put("getFirstName", "Saipriya");
                    put("getLastName", "Balasubramanian");
                    put("getEmail", "saipriya.balasubramanian@turbonomic.com");
                    put("getCustomerId", null);
                    put("getNumSockets", 200);
                    put("getVmTotal", 0);
                    put("getNumEntities", 200);
                    put("getEdition", null);
                    put("getExpirationDate", "2050-01-31");
                    put("getLockCode", "1944c723f8bcaf1ed6831a4f9d865794");
                    put("getCountedEntity", CountedEntity.SOCKET);
                    put("getFeatures", new TreeSet<>(Sets.union(
                        commonFeatures,
                        new TreeSet<>(Collections.singletonList("cloud_cost")))));
                }}
            },
            {"LicenseDeserializationTest_license_v2.xml",
                new HashMap<String, Object>() {{
                    put("getFirstName", "Giampiero");
                    put("getLastName", "De Ciantis");
                    put("getEmail", "gp.deciantis@turbonomic.com");
                    put("getCustomerId", null);
                    put("getNumSockets", 0);
                    put("getVmTotal", 10000);
                    put("getNumEntities", 10000);
                    put("getEdition", "Turbonomic Best");
                    put("getExpirationDate", "2022-08-18");
                    put("getLockCode", "92001124fe07e4f7d0f24cb27bf7c215");
                    put("getCountedEntity", CountedEntity.VM);
                    put("getFeatures", new TreeSet<>(Sets.union(
                        commonFeatures,
                        new TreeSet<>(Arrays.asList(
                            "API2",
                            "vdi_control",
                            "scaling",
                            "custom_policies",
                            "action_script",
                            "SLA"
                        )))));
                }}
            },
        };
    }

} // end class LicenseXmlDTOTest
