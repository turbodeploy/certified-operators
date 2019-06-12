package com.vmturbo.clustermgr;


import static com.vmturbo.clustermgr.ClusterMgrService.HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP;
import static com.vmturbo.clustermgr.ClusterMgrService.UPLOAD_VMTURBO_COM_URL;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.orbitz.consul.model.catalog.CatalogService;
import com.orbitz.consul.model.kv.Value;

import com.vmturbo.api.dto.admin.HttpProxyDTO;

;

/**
 * test for ClusterMgr Service
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
    classes = {ClusterMgrServiceTestConfiguration.class})
public class ClusterMgrServiceTest {

    // parms: componentType, propertyName
    private static final String COMPONENT_DEFAULT_PROPERTY_KEY = "vmturbo/components/%s/defaults/%s";
    // parms: componentType, instanceId
    private static final String COMPONENT_INSTANCE_NODE_NAME_KEY = "vmturbo/components/%s/instances/%s/node";
    // parms: componentType, instanceId, propertyName
    private static final String COMPONENT_INSTANCE_PROPERTY_KEY = "vmturbo/components/%s/instances/%s/properties/%s";
    @Autowired
    ConsulService consulServiceMock;
    @Autowired
    private ClusterMgrService clusterMgrService;
    private List<Value> mockValues = getMockConsulValues(
        "vmturbo/components/c1/", null,
        "vmturbo/components/c1/instances/c1_1/", null,
        "vmturbo/components/c1/instances/c1_1/properties/", null,
        "vmturbo/components/c1/instances/c1_1/node", "node1",
        "vmturbo/components/c1/instances/c1_1/properties/prop1", "val1",
        "vmturbo/components/c1/instances/c1_1/properties/prop2", "val2",
        "vmturbo/components/c1/", null,
        "vmturbo/components/c1/instances/c1_2/", null,
        "vmturbo/components/c1/instances/c1_2/properties/", null,
        "vmturbo/components/c1/instances/c1_2/node", "node2",
        "vmturbo/components/c1/instances/c1_2/properties/prop1", "val2",
        "vmturbo/components/c2/", null,
        "vmturbo/components/c2/instances/c2_1/", null,
        "vmturbo/components/c2/instances/c2_1/properties/", null,
        "vmturbo/components/c2/instances/c2_1/node", "node3",
        "vmturbo/components/c2/instances/c2_1/properties/prop3", "val3",
        "c1_1/component.version", "1.0.0",
        "c2_1/component.version", "1.1.0"
    );

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.reset(consulServiceMock);
        // note that we're returning ALL the values regardless of the query string; The code will reject the extra values
        when(consulServiceMock.getValues(anyString())).thenReturn(mockValues);
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_1/node", "default")).thenReturn("node1");
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_2/node", "default")).thenReturn("node2");
        when(consulServiceMock.getValueAsString("vmturbo/components/c2/instances/c2_1/node", "default")).thenReturn("node3");
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_1/node", "default")).thenReturn("node1");
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_2/node", "default")).thenReturn("node2");
        when(consulServiceMock.getValueAsString("vmturbo/components/c2/instances/c2_1/node", "default")).thenReturn("node3");
        when(consulServiceMock.getValueAsString("c1_1/component.version")).thenReturn(Optional.of("1.0.0"));
        when(consulServiceMock.getValueAsString("c1_2/component.version")).thenReturn(Optional.of("1.2.0"));
        when(consulServiceMock.getValueAsString("c2_1/component.version")).thenReturn(Optional.of("1.1.0"));
    }

    @Test
    public void getKnownComponentsTest() {
        // Arrange
        // Act
        Set<String> componentTypes = clusterMgrService.getKnownComponents();
        // Assert
        String[] expectedComponentTypes = {"c1", "c2"};
        assertThat(componentTypes, containsInAnyOrder(expectedComponentTypes));
    }

    @Test
    public void getClusterConfigurationTest() throws Exception {
        // Arrange
        // Act
        ClusterConfiguration testResult = clusterMgrService.getClusterConfiguration();
        // Assert
        String[] expectedProperties = {"prop1", "prop2"};
        assertThat(testResult.getInstances().get("c1_1").getProperties().keySet(), containsInAnyOrder(expectedProperties));
        assertThat(testResult.getInstances().get("c1_1").getProperties().get("prop1"), is("val1"));
    }


    @Test
    public void testGetComponentInstanceIds() throws Exception {
        // Arrange
        String[] expected = {"c1_1", "c1_2"};
        // Act
        Set<String> instanceIds = clusterMgrService.getComponentInstanceIds("c1");
        // Assert
        assertThat(instanceIds, containsInAnyOrder(expected));
    }

    @Test
    public void testGetPropertiesforComponentInstance() throws Exception {
        // Arrange
        // Act
        ComponentProperties propMap = clusterMgrService.getComponentInstanceProperties("c1", "c1_1");
        // Assert
        assertThat(propMap.size(), is(2));
        assertThat(propMap.get("prop1"), is("val1"));
        assertThat(propMap.get("prop2"), is("val2"));
    }

    @Test
    public void getComponentTypePropertyTest() throws Exception {
        // Arrange
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/defaults/p1"))
            .thenReturn(Optional.fromNullable("defaultVal1"));
        // Act
        String val = clusterMgrService.getComponentTypeProperty("c1", "p1");
        // Assert
        assertThat(val, is("defaultVal1"));
    }

    @Test
    public void putDefaultPropertiesForComponentTypeTest() throws Exception {
        // Arrange
        ComponentProperties newProperties = new ComponentProperties();
        newProperties.put("p1", "v1");
        newProperties.put("p2", "v2");
        String componentTypeKeyStem = "vmturbo/components/c1/";
        String defaultPropertiesKeyStem = componentTypeKeyStem + "defaults/";
        String componentInstanceKeyStem = componentTypeKeyStem + "instances/c1-1/";
        List<CatalogService> mockServiceList = new ArrayList<>();
        when(consulServiceMock.getService("c1")).thenReturn(mockServiceList);
        // Act
        clusterMgrService.putDefaultPropertiesForComponentType("c1", newProperties);
        // Assert
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valCaptor = ArgumentCaptor.forClass(String.class);

        verify(consulServiceMock, times(2)).putValue(keyCaptor.capture(), valCaptor.capture());
        assertThat(keyCaptor.getAllValues(), containsInAnyOrder(
            defaultPropertiesKeyStem + "p1",
            defaultPropertiesKeyStem + "p2"));
        assertThat(valCaptor.getAllValues(), containsInAnyOrder("v1", "v2"));

        // ensure that the type exists
        verify(consulServiceMock, times(1)).putValue(componentTypeKeyStem);

        // create the new instance
        verify(consulServiceMock, times(1)).keyExist(componentInstanceKeyStem);
        verify(consulServiceMock, times(1)).putValue(componentInstanceKeyStem);
        verify(consulServiceMock, times(1)).getKeys(componentInstanceKeyStem);

        // fetch the values
        verify(consulServiceMock).getKeys(defaultPropertiesKeyStem);
        verify(consulServiceMock, times(1)).getValues(defaultPropertiesKeyStem);
        verify(consulServiceMock, times(1)).getService("c1");
        verifyNoMoreInteractions(consulServiceMock);
    }

    @Test
    public void testGetPropertyforComponentInstance() throws Exception {
        // Arrange
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_1/properties/prop1"))
            .thenReturn(Optional.fromNullable("val1"));
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/instances/c1_1/properties/prop2"))
            .thenReturn(Optional.fromNullable("val2"));
        when(consulServiceMock.getValueAsString("vmturbo/components/c2/instances/c2_1/properties/prop1"))
            .thenReturn(Optional.fromNullable("val1b"));
        when(consulServiceMock.getValueAsString("vmturbo/components/c2/instances/c2_1/properties/prop2"))
            .thenReturn(Optional.fromNullable(null));
        // Act
        String prop1Val = clusterMgrService.getComponentInstanceProperty("c1", "c1_1", "prop1");
        String prop2Val = clusterMgrService.getComponentInstanceProperty("c1", "c1_1", "prop2");
        String prop1bVal = clusterMgrService.getComponentInstanceProperty("c2", "c2_1", "prop1");
        String prop2Null = clusterMgrService.getComponentInstanceProperty("c2", "c2_1", "prop2");
        // asssert
        assertThat(prop1Val, is("val1"));
        assertThat(prop2Val, is("val2"));
        assertThat(prop1bVal, is("val1b"));
        assertThat(prop2Null, nullValue());
    }

    @Test
    public void testSetProperty() throws Exception {
        // Arrange
        String componentType = "c1";
        String instanceId = "c1_1";
        String propertyName = "p1";
        String COMPONENT_INSTANCE_PROPERTY_FORMAT = "vmturbo/components/%s/instances/%s/properties/%s";
        String expectedKey = String.format(COMPONENT_INSTANCE_PROPERTY_FORMAT, componentType, instanceId, propertyName);
        String newValue = "new-value";
        when(consulServiceMock.getValueAsString(anyString())).thenReturn(Optional.fromNullable(newValue));
        // Act
        clusterMgrService.setPropertyForComponentInstance(componentType, instanceId, propertyName, newValue);
        // Assert
        verify(consulServiceMock, times(1)).putValue(expectedKey, newValue);
        verify(consulServiceMock, times(1)).getValueAsString(expectedKey);
        verify(consulServiceMock, times(1)).getServiceById(componentType, instanceId);
        verifyNoMoreInteractions(consulServiceMock);
    }

    @Test
    public void testGetNodeForComponentInstance() throws Exception {
        // Arrange
        // Act
        String result = clusterMgrService.getNodeForComponentInstance("c1", "c1_1");
        // Assert
        assertThat(result, is("node1"));
    }

    @Test
    public void testSetNodeForComponentInstance() throws Exception {
        // Arrange
        String expected = "vmturbo/components/c1/instances/c1_1/node";
        when(consulServiceMock.getValueAsString(expected)).thenReturn(Optional.of("test-node"));
        // Act
        clusterMgrService.setNodeForComponentInstance("c1", "c1_1", "test-node");
        // Assert
        verify(consulServiceMock, times(1)).putValue(expected, "test-node");
    }

    @Test
    public void testSetClusterConfiguration() throws Exception {
        // Arrange
        InputStream clusterConfigJson = getClass().getClassLoader().getResourceAsStream("clusterConfigurationTest.json");
        ClusterConfiguration testClusterConfiguration = new ObjectMapper().readValue(clusterConfigJson, ClusterConfiguration.class);
        // Act
        clusterMgrService.setClusterConfiguration(testClusterConfiguration);
        // Assert
        verify(consulServiceMock).deleteKey("vmturbo/");
        verify(consulServiceMock).putValue(getDefaultPropertyKey("c1", "prop1"), "val1");
        verify(consulServiceMock).putValue(getInstanceNodeKey("c1", "c1_1"), "node1");
        verify(consulServiceMock).putValue(getInstanceNodeKey("c2", "c2_1"), "node2");
        verify(consulServiceMock).putValue(getInstancePropertyKey("c1", "c1_1", "prop1"), "val1");
        verify(consulServiceMock).putValue(getInstancePropertyKey("c1", "c1_1", "prop2"), "val2");
        verify(consulServiceMock).putValue(getInstancePropertyKey("c2", "c2_1", "prop3"), "val3");
        // etc
    }

    private List<Value> getMockConsulValues(String... keyValuePairs) {
        List<Value> values = new ArrayList<>();
        Iterator<String> i = Arrays.asList(keyValuePairs).iterator();
        while (i.hasNext()) {
            String key = i.next();
            String val = i.next();
            values.add(getMockConsulValue(key, val));
        }
        return values;
    }

    private Value getMockConsulValue(String key, String val) {
        Value v = Mockito.mock(Value.class);
        when(v.getKey()).thenReturn(key);
        if (val != null) {
            when(v.getValueAsString()).thenReturn(Optional.of(val));
        } else {
            when(v.getValueAsString()).thenReturn(Optional.absent());
        }
        return v;
    }

    /**
     * Construct a key stem for values on a given node.
     *
     * @param componentType the type for this component
     * @param componentId   the instance id of this component
     */
    @Nonnull
    private String getInstanceNodeKey(String componentType, String componentId) {
        return String.format(COMPONENT_INSTANCE_NODE_NAME_KEY, componentType, componentId);
    }

    /**
     * Construct a key stem for a given property of a given component on a given node.
     *
     * @param propertyName the name of the property to be retrieved
     * @return a key stem for a node/component/property specifier
     */
    @Nonnull
    private String getInstancePropertyKey(
        @Nonnull String componentType,
        @Nonnull String instanceId,
        @Nonnull String propertyName) {
        return String.format(COMPONENT_INSTANCE_PROPERTY_KEY, componentType, instanceId, propertyName);
    }

    /**
     * Construct a key stem for a given property of a given component type.
     *
     * @param componentType the node where this component resides
     * @param propertyName  the name of the property to be retrieved
     * @return a key stem for a node/component/property specifier
     */
    @Nonnull
    private String getDefaultPropertyKey(
        @Nonnull String componentType,
        @Nonnull String propertyName) {
        return String.format(COMPONENT_DEFAULT_PROPERTY_KEY, componentType, propertyName);
    }

    @Test
    public void testValidateDiagsName() {
        final long currentEpoch = System.currentTimeMillis();
        assertEquals("/home/turbonomic/data/turbonomic-diags-_" + currentEpoch + ".zip",
            String.format(HOME_TURBONOMIC_DATA_TURBO_FILE_ZIP, currentEpoch));
    }

    @Test
    public void testGetCurlArgs() {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.10");
        dto.setProxyPortNumber(1080);
        String[] curlArgs = clusterMgrService.getCurlArgs("/home/turbonomic/data/turbonomic-diags-_111.zip", dto);
        String[] expectedArgs = {"-F", "ufile=@/home/turbonomic/data/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL};
        // since proxy is not enabled, so proxy settings should be ignored
        assertArrayEquals(expectedArgs, curlArgs);
        dto.setIsProxyEnabled(true);
        dto.setUserName("user");
        curlArgs = clusterMgrService.getCurlArgs("/home/turbonomic/data/turbonomic-diags-_111.zip", dto);
        String[] expectedArgsWithProxy = {"-F", "ufile=@/home/turbonomic/data/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL, "-x", "10.10.10.10:1080" };
        assertArrayEquals(expectedArgsWithProxy, curlArgs);
        dto.setPassword("password");
        curlArgs = clusterMgrService.getCurlArgs("/home/turbonomic/data/turbonomic-diags-_111.zip", dto);
        String[] expectedArgsWithSecureProxy = {"-F", "ufile=@/home/turbonomic/data/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL, "-x", "user:password@10.10.10.10:1080"};
        assertArrayEquals(expectedArgsWithSecureProxy, curlArgs);
    }
}