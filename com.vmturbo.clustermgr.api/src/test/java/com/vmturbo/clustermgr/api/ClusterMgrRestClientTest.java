package com.vmturbo.clustermgr.api;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * Class to exercise the ClusterMgr REST API entrypoints. Most of these entrypoints forward requests directly to
 * ClusterMgr using {@link RestTemplate}.exchange(). The MockRestServiceServer sets up mocking for the normal
 * RestTemplate operation.
 */
public class ClusterMgrRestClientTest {

    private ClusterMgrRestClient testRestClient;
    private MockRestServiceServer mockServer;
    private String baseTestURL;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TELEMETRY_INITIALIZED = "/proactive/initialized";
    private static final String TELEMETRY_ENABLED = "/proactive/enabled";

    /**
     * Create mock rest server and an instance of ClusterMgrRestClient to be used in all tests.
     */
    @Before
    public void setUp() {
        // Create a connection config to pass to the ClusterMgrRestClient under test
        ComponentApiConnectionConfig connectionConfig = ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort("test-host", 4321, "")
                .build();
        baseTestURL = "http://test-host:4321";
        // Create the class to test
        testRestClient = new ClusterMgrRestClient(connectionConfig);
        mockServer = MockRestServiceServer.createServer(testRestClient.getRestTemplate());
    }

    /**
     * Check that the call to check 'isXLEnabled()' indicates that this is XL running.
     */
    @Test
    public void testIsXLEnabled() {
        // Act
        boolean result = testRestClient.isXLEnabled();
        // Assert
        assertTrue(result);
    }

    /**
     * Check that a ClusterConfiguration response is fetched and correctly deserialized.
     */
    @Test
    public void testGetClusterConfiguration() {
        // Arrange
        String expectedUri = baseTestURL;
        ClusterConfiguration testConfiguration = getSampleClusterConfiguration();
        String testResponseString = writeValueAsJsonString(testConfiguration);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ClusterConfiguration result = testRestClient.getClusterConfiguration();
        // Assert
        mockServer.verify();
        assertThat(result, equalTo(testConfiguration));
    }

    /**
     * Check setting ClusterConfiguration returns the original configuration and is correctly
     * deserialized.
     */
    @Test
    public void testSetClusterConfiguration() {
        // Arrange
        String expectedUri = baseTestURL;
        ClusterConfiguration expected = getSampleClusterConfiguration();
        String testResponseString = writeValueAsJsonString(expected);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ClusterConfiguration result = testRestClient.setClusterConfiguration(expected);
        // Assert
        mockServer.verify();
        assertThat(result, equalTo(expected));
    }

//    /**
//     * Test fetching instance ids.
//     */
//    @Test
//    public void testGetComponentInstanceIds() {
//        // Arrange
//        final String expectedUri = baseTestURL + "/components/component-type/instances";
//        String[] instanceIds = {"instance-1", "instance-2"};
//        String testResponseString = writeValueAsJsonString(instanceIds);
//        mockServer.expect(requestTo(expectedUri))
//            .andExpect(method(HttpMethod.GET))
//            .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
//        // Act
//        Set<String> resultInstanceIds = testRestClient.getComponentInstanceIds("component-type");
//        // Assert
//        assertThat(resultInstanceIds, containsInAnyOrder("instance-1", "instance-2"));
//    }

    /**
     * Test dispatching the request to fetch the list of known component types from the
     * configuration.
     */
    @Test
    public void testGetKnownComponents() {
        // Arrange
        String expectedUri = baseTestURL + "/components";
        Set<String> testResponse = new HashSet<>();
        String testResponseString = writeValueAsJsonString(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Set<String> result = testRestClient.getKnownComponents();
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test dispatching the request to set a component instance property.
     */
    @Test
    public void testSetPropertyForComponentInstance() {
        // Arrange
        String newValue = "mock-post-value";
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties/test-property";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(newValue))
                .andRespond(withSuccess(newValue, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.setComponentInstanceProperty("test-type", "test-instance",
            "test-property", newValue);
        // Assert
        mockServer.verify();
        assertThat(result, is(newValue));
    }

    /**
     * Test the dispatch for fetching component instance ids.
     */
    @Test
    public void testGetComponentInstanceIds() {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/instances";
        Set<String> testResponse = Sets.newHashSet("a", "b", "c");
        String testResponseString = writeValueAsJsonString(testResponse);
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Set<String> result = testRestClient.getComponentInstanceIds("test-type");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch for setting a component-type local property.
     */
    @Test
    public void testSetComponentLocalProperty() {
        // arrange
        String newValue = "mock-post-value";
        String expectedUri = baseTestURL + "/components/test-type/localProperties/test-property";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.PUT))
            .andExpect(content().string(newValue))
            .andRespond(withSuccess(newValue, MediaType.APPLICATION_JSON_UTF8));
        // act
        final String result = testRestClient.setComponentLocalProperty("test-type",
            "test-property", newValue);
        // assert
        assertThat(result, equalTo(newValue));
    }

    /**
     * Test the dispatch for deleting a component-type local property.
     */
    @Test
    public void testDeleteComponentLocalProperty() {
        // arrange
        String oldValue = "mock-post-value";
        String expectedUri = baseTestURL + "/components/test-type/localProperties/test-property";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.DELETE))
            .andRespond(withSuccess(oldValue, MediaType.APPLICATION_JSON_UTF8));
        // act
        final String result = testRestClient.deleteComponentLocalProperty("test-type",
            "test-property");
        // assert
        assertThat(result, equalTo(oldValue));
    }

    /**
     * Test the dispatch for a request to get the "effective" properties for a component instance.
     */
    @Test
    public void testGetEffectiveInstanceProperties() {
        // arrange
        final ComponentProperties expectedResult = getSampleComponentProperties("prop_");
        String returnString = writeValueAsJsonString(expectedResult);
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/effectiveProperties";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(returnString, MediaType.APPLICATION_JSON_UTF8));
        // act
        final ComponentProperties result = testRestClient.getEffectiveInstanceProperties("test-type",
            "test-instance");
        // assert
        assertThat(result, equalTo(expectedResult));
    }

    /**
     * Test the dispatch for a request to fetch a "default" property for a component type.
     */
    @Test
    public void testGetComponentDefaultProperty() {
        // arrange
        final String expectedResult = "expected value";
        String expectedUri = baseTestURL + "/components/test-type/defaults/prop-name";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(expectedResult, MediaType.APPLICATION_JSON_UTF8));
        // act
        final String result = testRestClient.getComponentDefaultProperty("test-type", "prop-name");
        // assert
        assertThat(result, equalTo(expectedResult));
    }

    /**
     * Test the dispatch for a request to fetch a "local" property for a component type.
     */
    @Test
    public void testGetComponentLocqlProperty() {
        // arrange
        final String expectedResult = "expected value";
        String expectedUri = baseTestURL + "/components/test-type/localProperties/prop-name";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(expectedResult, MediaType.APPLICATION_JSON_UTF8));
        // act
        final String result = testRestClient.getComponentLocalProperty("test-type", "prop-name");
        // assert
        assertThat(result, equalTo(expectedResult));
    }

    /**
     * Test the dispatch for a request to fetch all the "local" properties for a component type.
     */
    @Test
    public void testGetCurrentComponentProperties() {
        // arrange
        final ComponentProperties expectedResult = getSampleComponentProperties("prop_");
        String returnString = writeValueAsJsonString(expectedResult);
        String expectedUri = baseTestURL + "/components/test-type/localProperties";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(returnString, MediaType.APPLICATION_JSON_UTF8));
        // act
        final ComponentProperties result = testRestClient.getComponentLocalProperties("test-type");
        // assert
        assertThat(result, equalTo(expectedResult));
    }

    /**
     * Test the dispatch for a request to update all the properties for a component instance.
     */
    @Test
    public void testPutComponentInstanceProperties() {
        // arrange
        final ComponentProperties expectedResult = getSampleComponentProperties("prop_");
        String returnString = writeValueAsJsonString(expectedResult);
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.PUT))
            .andRespond(withSuccess(returnString, MediaType.APPLICATION_JSON_UTF8));
        // act
        final ComponentProperties result = testRestClient.putComponentInstanceProperties(
            "test-type", "test-instance", expectedResult);
        // assert
        assertThat(result, equalTo(expectedResult));
    }

    /**
     * Test the dispatch for a request to get the current execution stat for all component types.
     */
    @Test
    public void testGetComponentsState() {
        // Arrange
        String expectedUri = baseTestURL + "/state";
        Map<String, String> testResponse = Maps.newHashMap();
        testResponse.put("a", "new");
        testResponse.put("b", "running");
        String testResponseString = writeValueAsJsonString(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Map<String, String> result = testRestClient.getComponentsState();
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch and return decoding for a request to dump diagnostics.
     *
     * @throws UnsupportedEncodingException If there is an error encoding the zip data
     */
    @Test
    public void testCollectComponentDiagnostics() throws UnsupportedEncodingException {
        // Arrange
        String expectedUri = baseTestURL + "/diagnostics";
        String testResponse = "this is a zipped file";
        mockServer = MockRestServiceServer.createServer(testRestClient.getStreamingRestTemplate());
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, new MediaType("application", "zip")));
        ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();
        // Act
        testRestClient.collectComponentDiagnostics(testOutputStream);
        // Assert
        mockServer.verify();
        assertThat(testOutputStream.toString(StandardCharsets.UTF_8.name()), is(testResponse));
    }

    /**
     * Test dispatch for a request to fetch the default properties for a component type.
     */
    @Test
    public void testGetDefaultPropertiesForComponentType() {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/defaults";
        ComponentProperties testResponse = new ComponentProperties();
        String testResponseString = writeValueAsJsonString(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentProperties result = testRestClient.getComponentDefaultProperties("test-type");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch for a request to store new component properties for a component type.
     */
    @Test
    public void testPutDefaultPropertiesForComponentType() {
        // Arrange
        ComponentProperties testResponse = new ComponentProperties();
        String testResponseString = writeValueAsJsonString(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/defaults";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(testResponseString))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentProperties result = testRestClient.putComponentDefaultProperties("test-type",
                testResponse);
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch for a request to get the instance properties for a component instance.
     */
    @Test
    public void testGetComponentInstanceProperties() {
        // Arrange
        ComponentProperties testResponse = new ComponentProperties();
        String testResponseString = writeValueAsJsonString(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentProperties result = testRestClient.getComponentInstanceProperties("test-type", "test-instance");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch for a request to update the local properties for a component type.
     */
    @Test
    public void testPutComponentLocalProperties() {
        // Arrange
        ComponentProperties testResponse = new ComponentProperties();
        String testResponseString = writeValueAsJsonString(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/localProperties";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(testResponseString))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentProperties result = testRestClient.putLocalComponentProperties("test-type",
            testResponse);
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    //    @Test
//    public void testGetComponentTypeProperty() throws Exception {
//        // Arrange
//        String expectedUri = baseTestURL + "/components/test-type/defaults/test-property";
//        String testResponse = "propertyValue";
//        mockServer.expect(requestTo(expectedUri))
//                .andExpect(method(HttpMethod.GET))
//                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
//        // Act
//        String result = testRestClient.getComponentDefaultProperty("test-type", "test-property");
//        // Assert
//        mockServer.verify();
//        assertThat(result, is(testResponse));
//    }

    /**
     * Test the dispatch for a request to fetch a component instance property.
     */
    @Test
    public void testGetComponentInstanceProperty() {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties/test-property";
        String testResponse = "propertyValue";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.getComponentInstanceProperty("test-type", "test-instance", "test-property");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    /**
     * Test the dispatch for a call to fetch the setting status for telemetry initialized.
     */
    @Test
    public void testIsTelemetryInitialized() {
        // Arrange
        String expectedUri = baseTestURL + TELEMETRY_INITIALIZED;
        String testResponse = "true";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Boolean result = testRestClient.isTelemetryInitialized();
        // Assert
        mockServer.verify();
        assertThat(result, is(Boolean.valueOf(testResponse)));
    }

    /**
     * Test the dispatch for the request to fetch the setting for telemetry enabled == true.
     */
    @Test
    public void testIsTelemetryEnabledTrue() {
        // Arrange
        String expectedUri = baseTestURL + TELEMETRY_ENABLED;
        String testResponse = "true";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Boolean result = testRestClient.isTelemetryEnabled();
        // Assert
        mockServer.verify();
        assertThat(result, is(Boolean.valueOf(testResponse)));
    }

    /**
     * Test the dispatch for the request to fetch the setting for telemetry enabled == false.
     */
    @Test
    public void testIsTelemetryEnabledFalse() {
        // Arrange
        String expectedUri = baseTestURL + TELEMETRY_ENABLED;
        String testResponse = "false";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Boolean result = testRestClient.isTelemetryEnabled();
        // Assert
        mockServer.verify();
        assertThat(result, is(Boolean.valueOf(testResponse)));
    }

    /**
     * Test the dispatch for the request to set the telemetry enabled status.
     */
    @Test
    public void testSetTelemetryEnabled() {
        // Arrange
        final boolean newValue = false;
        String newValueString = writeValueAsJsonString(newValue);
        String expectedUri = baseTestURL + TELEMETRY_ENABLED;
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(newValueString))
                .andRespond(withSuccess(newValueString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        testRestClient.setTelemetryEnabled(newValue);
        // Assert
        mockServer.verify();
    }

    /**
     * Test the request to initiate component diagnostics dump.
     */
    @Test
    public void testExportComponentDiagnostics() {
        // Arrange
        final HttpProxyConfig httpProxyDTO = new HttpProxyConfig(true, "proxyHost",
            9876, "proxyUser", "secret");
//        final String postString = writeValueAsJsonString(httpProxyDTO); use when .andExpect works
        String expectedUri = baseTestURL + "/diagnostics";
        mockServer.expect(requestTo(expectedUri))
            .andExpect(method(HttpMethod.POST))
//            .andExpect(content().json(postString)) not implemented in this release of spring
            .andRespond(withSuccess("true", MediaType.APPLICATION_JSON_UTF8));
        // Act
        boolean result = testRestClient.exportComponentDiagnostics(httpProxyDTO);
        // Assert
        assertTrue(result);
    }

    // for OM-50555
    @Test
    public void testJacksonSerialization() throws IOException {
        HttpProxyConfig proxyConfig = new HttpProxyConfig(false, null, null, null, null);
        // serialize
        ObjectMapper writeMapper = new ObjectMapper();
        String serializedObject = writeMapper.writeValueAsString(proxyConfig);

        // deserialize
        ObjectMapper readMapper = new ObjectMapper();
        readMapper.readValue(serializedObject, HttpProxyConfig.class);
    }

    private String writeValueAsJsonString(final Object objectToConvert) {
        try {
            return objectMapper.writeValueAsString(objectToConvert);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting " + objectToConvert + " to JSON string: ", e);
        }
    }

    private ClusterConfiguration getSampleClusterConfiguration() {
        ClusterConfiguration testConfiguration = new ClusterConfiguration();
        final String componentType = "componentType";
        testConfiguration.addComponentInstance("instanceId", componentType, "v0.0.1",
            getSampleComponentProperties("instance_prop_"));
        testConfiguration.addComponentType(componentType, getSampleComponentProperties("default_"));
        return testConfiguration;
    }

    private ComponentProperties getSampleComponentProperties(final String propertyPrefix) {
        final ComponentProperties expectedResult = new ComponentProperties();
        expectedResult.put(propertyPrefix + "key1", "val1");
        expectedResult.put(propertyPrefix + "key2", "val2");
        return expectedResult;
    }
}