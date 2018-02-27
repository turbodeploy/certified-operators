package com.vmturbo.clustermgr.api.impl;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import org.junit.Before;
import org.junit.Test;

/**
 * Class to exercise the ClusterMgr REST API entrypoints. Most of these entrypoints forward requests directly to
 * ClusterMgr using {@link RestTemplate}.exchange(). The MockRestServiceServer sets up mocking for the normal
 * RestTemplate operation.
 */
public class ClusterMgrRestClientTest {

    private ClusterMgrRestClient testRestClient;
    private MockRestServiceServer mockServer;
    private String baseTestURL;

    private Gson gson = new Gson();

    private static Logger logger = LogManager.getLogger();

    private static String TELEMETRY_INITIALIZED = "/proactive/initialized";
    private static String TELEMETRY_ENABLED = "/proactive/enabled";

    @Before
    public void setUp() throws Exception {
        // Create a connection config to pass to the ClusterMgrRestClient under test
        ComponentApiConnectionConfig connectionConfig = ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort("test-host", 4321)
                .build();
        baseTestURL = "http://test-host:4321/api/v2";
        // Create the class to test
        testRestClient = new ClusterMgrRestClient(connectionConfig);
        mockServer = MockRestServiceServer.createServer(testRestClient.getRestTemplate());
//        mockServer.bindTo(testRestClient.getRestTemplate());
    }

    @Test
    public void testIsXLEnabled() throws Exception {
        // Act
        boolean result = testRestClient.isXLEnabled();
        // Assert
        assertTrue(result);
    }

    @Test
    public void testGetClusterConfiguration() throws Exception {
        // Arrange
        String expectedUri = baseTestURL;
        ClusterConfigurationDTO testResponse = new ClusterConfigurationDTO();
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ClusterConfigurationDTO result = testRestClient.getClusterConfiguration();
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testSetClusterConfiguration() throws Exception {

        // Arrange
        String expectedUri = baseTestURL;
        ClusterConfigurationDTO postMockDTO = new ClusterConfigurationDTO();
        ClusterConfigurationDTO testResponse = new ClusterConfigurationDTO();
        String testPostString = gson.toJson(postMockDTO);
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ClusterConfigurationDTO result = testRestClient.setClusterConfiguration(postMockDTO);
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testGetKnownComponents() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/components";
        Set<String> testResponse = new HashSet<>();
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Set<String> result = testRestClient.getKnownComponents();
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testSetPropertyForComponentType() throws Exception {
        // Arrange
        String newValue = "mock-post-value";
        String expectedUri = baseTestURL + "/components/test-type/defaults/test-property";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(newValue))
                .andRespond(withSuccess(newValue, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.setPropertyForComponentType("test-type", "test-property", newValue);
        // Assert
        mockServer.verify();
        assertThat(result, is(newValue));
    }

    @Test
    public void testSetPropertyForComponentInstance() throws Exception {
        // Arrange
        String newValue = "mock-post-value";
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties/test-property";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(newValue))
                .andRespond(withSuccess(newValue, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.setPropertyForComponentInstance("test-type", "test-instance", "test-property", newValue);
        // Assert
        mockServer.verify();
        assertThat(result, is(newValue));
    }

    @Test
    public void testGetComponentInstanceIds() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/instances";
        Set<String> testResponse = Sets.newHashSet("a", "b", "c");
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Set<String> result = testRestClient.getComponentInstanceIds("test-type");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testGetComponentsState() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/state";
        Map<String, String> testResponse = Maps.newHashMap();
        testResponse.put("a", "new");
        testResponse.put("b", "running");
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        Map<String, String> result = testRestClient.getComponentsState();
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testCollectComponentDiagnostics() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/diagnostics";
        String testResponse = "this is a zipped file";
        mockServer = MockRestServiceServer.createServer(testRestClient.getStreamingRestTemplate());
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, new MediaType("application","zip")));
        ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();
        // Act
        testRestClient.collectComponentDiagnostics(testOutputStream);
        // Assert
        mockServer.verify();
        assertThat(testOutputStream.toString(StandardCharsets.UTF_8.name()), is(testResponse));
    }

    @Test
    public void testGetNodeForComponentInstance() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/node";
        String testResponse = "node-name";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.getNodeForComponentInstance("test-type", "test-instance");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testSetNodeForComponentInstance() throws Exception {
        // Arrange
        String newValue = "test-node";
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/node";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(newValue))
                .andRespond(withSuccess(newValue, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.setNodeForComponentInstance("test-type", "test-instance", newValue);
        // Assert
        mockServer.verify();
        assertThat(result, is(newValue));
    }

    @Test
    public void testGetDefaultPropertiesForComponentType() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/defaults";
        ComponentPropertiesDTO testResponse = new ComponentPropertiesDTO();
        String testResponseString = gson.toJson(testResponse);
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentPropertiesDTO result = testRestClient.getDefaultPropertiesForComponentType("test-type");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testPutDefaultPropertiesForComponentType() throws Exception {
        // Arrange
        ComponentPropertiesDTO newValue = new ComponentPropertiesDTO();
        String newValueString = gson.toJson(newValue);
        ComponentPropertiesDTO testResponse = new ComponentPropertiesDTO();
        String testResponseString = gson.toJson(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/defaults";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(testResponseString))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentPropertiesDTO result = testRestClient.putDefaultPropertiesForComponentType("test-type", testResponse);
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testGetComponentInstanceProperties() throws Exception {
        // Arrange
        ComponentPropertiesDTO testResponse = new ComponentPropertiesDTO();
        String testResponseString = gson.toJson(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentPropertiesDTO result = testRestClient.getComponentInstanceProperties("test-type", "test-instance");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testPutComponentInstanceProperties() throws Exception {
        // Arrange
        ComponentPropertiesDTO newValue = new ComponentPropertiesDTO();
        String newValueString = gson.toJson(newValue);
        ComponentPropertiesDTO testResponse = new ComponentPropertiesDTO();
        String testResponseString = gson.toJson(testResponse);
        String expectedUri = baseTestURL + "/components/test-type/instances/test-instance/properties";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.PUT))
                .andExpect(content().string(testResponseString))
                .andRespond(withSuccess(testResponseString, MediaType.APPLICATION_JSON_UTF8));
        // Act
        ComponentPropertiesDTO result = testRestClient.putComponentInstanceProperties("test-type", "test-instance", testResponse);
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testGetComponentTypeProperty() throws Exception {
        // Arrange
        String expectedUri = baseTestURL + "/components/test-type/defaults/test-property";
        String testResponse = "propertyValue";
        mockServer.expect(requestTo(expectedUri))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(testResponse, MediaType.APPLICATION_JSON_UTF8));
        // Act
        String result = testRestClient.getComponentTypeProperty("test-type", "test-property");
        // Assert
        mockServer.verify();
        assertThat(result, is(testResponse));
    }

    @Test
    public void testGetComponentInstanceProperty() throws Exception {
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

    @Test
    public void testIsTelemetryInitialized() throws Exception {
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

    @Test
    public void testIsTelemetryEnabledTrue() throws Exception {
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

    @Test
    public void testIsTelemetryEnabledFalse() throws Exception {
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

    @Test
    public void testSetTelemetryEnabled() throws Exception {
        // Arrange
        boolean newValue = false;
        String newValueString = gson.toJson(newValue);
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

}