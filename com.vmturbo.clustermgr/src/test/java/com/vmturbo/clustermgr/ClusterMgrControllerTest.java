package com.vmturbo.clustermgr;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;

/**
 * Tests for the ClusterMgr HTTP endpoints
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath:clustermgr-controller-test.xml")
// Refresh spring context between methods.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class ClusterMgrControllerTest {

    private static final String API_PREFIX="";

    protected static MockMvc mockMvc;

    @Autowired
    private ClusterMgrController clusterMgrController;

    @Autowired
    private ClusterMgrService clusterMgrServiceMock;

    @Autowired
    private CompositeHealthMonitor compositeHealthMonitor;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        compositeHealthMonitor.addHealthCheck(new HealthStatusProvider() {
            @Override
            public String getName() {
                return "test";
            }

            @Override
            public HealthStatus getHealthStatus() {
                return new SimpleHealthStatus(true, "good");
            }
        });
    }

    @Test
    public void getClusterConfiguration() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get("/")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getClusterConfiguration();
    }

    @Test
    public void setClusterConfiguration() throws Exception {
        // Arrange
        String sampleJson;
        try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("sample-cluster-config.json"))
        {
            sampleJson = IOUtils.toString(inputStream);
        }

        // Act
        MvcResult result = mockMvc.perform(put("/")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(sampleJson))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).setClusterConfiguration(anyObject());
    }

    @Test
    public void getDefaultsForComponentType() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/components/c1/defaults"))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getDefaultPropertiesForComponentType("c1");
        verifyNoMoreInteractions(clusterMgrServiceMock);
    }

    @Test
    public void getPropertiesForComponentInstance() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/components/c1/instances/c1_1/properties"))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getComponentInstanceProperties("c1", "c1_1");
        verifyNoMoreInteractions(clusterMgrServiceMock);
    }

    @Test
    public void getPropertyValuesTest() throws Exception {
        // Arrange
        String newValue = "new-value";
        when(clusterMgrServiceMock.getComponentInstanceProperty("c1", "c1_1", "propName")).thenReturn(newValue);

        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/components/c1/instances/c1_1/properties/propName")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(newValue))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getComponentInstanceProperty("c1", "c1_1", "propName");
        verifyNoMoreInteractions(clusterMgrServiceMock);
        assertThat(result.getResponse().getContentAsString(), is(newValue));

    }

    @Test
    public void setPropertyValuesTest() throws Exception {
        // Arrange
        String newValue = "new-value";
        when(clusterMgrServiceMock.setPropertyForComponentInstance("c1", "c1_1", "propName", newValue)).thenReturn(newValue);
        // Act
        MvcResult result = mockMvc.perform(put(API_PREFIX + "/components/c1/instances/c1_1/properties/propName")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(newValue))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).setPropertyForComponentInstance("c1", "c1_1", "propName", newValue);
        verifyNoMoreInteractions(clusterMgrServiceMock);
        assertThat(result.getResponse().getContentAsString(), is(newValue));
    }

    @Test
    public void getComponents() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/components")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getKnownComponents();
        verifyNoMoreInteractions(clusterMgrServiceMock);
    }

    @Test
    public void getDiagnostics() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/diagnostics")
                .accept("application/zip"))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).collectComponentDiagnostics(anyObject());
        verifyNoMoreInteractions(clusterMgrServiceMock);
    }

    @Test
    public void getState() throws Exception {
        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/state"))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(clusterMgrServiceMock, times(1)).getComponentsState();
        verifyNoMoreInteractions(clusterMgrServiceMock);
    }

    @Test
    public void getHealth() throws Exception {
        when(clusterMgrServiceMock.isClusterKvStoreInitialized()).thenReturn(true);

        // Arrange
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health"))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        assertThat(result.getResponse().getStatus(), is(200));
    }

    /**
     * Test that when the KV values are not initialized the clustermgr does not
     * report itself as healthy.
     */
    @Test
    public void getHealthKvNotInitialized() throws Exception {
        when(clusterMgrServiceMock.isClusterKvStoreInitialized()).thenReturn(false);

        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health"))
                .andExpect(status().isServiceUnavailable())
                .andReturn();
        assertThat(result.getResponse().getStatus(), is(503));
    }

    @Test
    public void testTelemetryInitializedTrue() throws Exception {
        when(clusterMgrServiceMock.isTelemetryEnabled()).thenReturn(true);

        MvcResult result = mockMvc.perform(get(API_PREFIX + "/proactive/enabled"))
                .andExpect(status().isOk())
                .andReturn();
        assertThat(result.getResponse().getContentAsString(), is("true"));

        verify(clusterMgrServiceMock).isTelemetryEnabled();
    }
    @Test

    public void testTelemetryInitializedFalse() throws Exception {
        when(clusterMgrServiceMock.isTelemetryEnabled()).thenReturn(false);

        MvcResult result = mockMvc.perform(get(API_PREFIX + "/proactive/enabled"))
                .andExpect(status().isOk())
                .andReturn();
        assertThat(result.getResponse().getContentAsString(), is("false"));

        verify(clusterMgrServiceMock).isTelemetryEnabled();
    }
}