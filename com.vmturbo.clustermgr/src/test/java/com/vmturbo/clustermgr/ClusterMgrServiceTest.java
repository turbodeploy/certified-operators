package com.vmturbo.clustermgr;

import static com.vmturbo.clustermgr.ClusterMgrService.UPLOAD_VMTURBO_COM_URL;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.orbitz.consul.model.kv.Value;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;

import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.clustermgr.api.HttpProxyConfig;

/**
 * Test for ClusterMgr Service.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
    classes = {ClusterMgrServiceTestConfiguration.class})
public class ClusterMgrServiceTest {

    @Autowired
    ConsulService consulServiceMock;
    @Autowired
    private ClusterMgrService clusterMgrService;
    private List<Value> mockValues = getMockConsulValues(
        "vmturbo/components/c1/defaults/p1", null,
        "vmturbo/components/c1/instances/c1_1/properties/p1", null,
        "vmturbo/components/c1/instances/c1_1/properties/prop1", "val1",
        "vmturbo/components/c1/instances/c1_1/properties/prop2", "val2",
        "vmturbo/components/c1/instances/c1_2/properties/prop1", "val2",
        "vmturbo/components/c2/instances/c2_1/properties/prop1", null,
        "vmturbo/components/c2/instances/c2_1/properties/prop2", null,
        "vmturbo/components/c2/instances/c2_1/properties/prop3", "val3",
        "vmturbo/components/c1/instances/c1_1/properties/component.version", "1.0.0",
        "vmturbo/components/c1/instances/c1_2/properties/component.version", "1.2.0",
        "vmturbo/components/c2/instances/c2_1/properties/component.version", "1.1.0"
    );

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.reset(consulServiceMock);
        // note that we're returning ALL the values regardless of the query string; The code will reject the extra values
        when(consulServiceMock.getValues(anyString())).thenReturn(mockValues);
    }

    @Test
    public void getKnownComponentsTest() {
        // Arrange
        final String c1 = "c1";
        final String c2 = "c2";
        when(consulServiceMock.getAllServiceInstances())
                .thenReturn(ImmutableMap.of(c1, Collections.EMPTY_LIST, c2, Collections.EMPTY_LIST));
        // Act
        Set<String> componentTypes = clusterMgrService.getKnownComponents();
        // Assert
        String[] expectedComponentTypes = {c1, c2};
        assertThat(componentTypes, containsInAnyOrder(expectedComponentTypes));
    }

    @Test
    public void testGetComponentInstanceIds() {
        // Arrange
        String[] expected = {"vmturbo", "c1_1", "c1_2"};
        // Act
        Set<String> instanceIds = clusterMgrService.getComponentInstanceIds("c1");
        // Assert
        assertThat(instanceIds, containsInAnyOrder(expected));
    }

    @Test
    public void testGetPropertiesforComponentInstance() {
        // Arrange
        // Act
        ComponentProperties propMap = clusterMgrService.getComponentInstanceProperties("c1", "c1_1");
        // Assert
        assertThat(propMap.size(), is(4));
        assertThat(propMap.get("prop1"), is("val1"));
        assertThat(propMap.get("prop2"), is("val2"));
    }

    @Test
    public void getComponentTypePropertyTest() {
        // Arrange
        when(consulServiceMock.getValueAsString("vmturbo/components/c1/defaults/p1"))
            .thenReturn(Optional.fromNullable("defaultVal1"));
        // Act
        String val = clusterMgrService.getDefaultComponentProperty("c1", "p1");
        // Assert
        assertThat(val, is("defaultVal1"));
    }

    @Test
    public void putDefaultPropertiesForComponentTypeTest() {
        // Arrange
        ComponentProperties newProperties = new ComponentProperties();
        newProperties.put("p1", "v1");
        newProperties.put("p2", "v2");
        String componentTypeKeyStem = "vmturbo/components/c1/";
        String defaultPropertiesKeyStem = componentTypeKeyStem + "defaults/";
        when(consulServiceMock.getValueAsString("c1-1/component.version"))
            .thenReturn(Optional.fromNullable("1.0.0"));

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

        // fetch the values
        verify(consulServiceMock).getKeys(defaultPropertiesKeyStem);
        verify(consulServiceMock).getValues(defaultPropertiesKeyStem);
        verify(consulServiceMock).getValueAsString("c1-1/component.version");
    }

    @Test
    public void testGetPropertyforComponentInstance() {
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
    public void testSetProperty() {
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
        verifyNoMoreInteractions(consulServiceMock);
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

    @Test
    public void testGetCurlArgs() {
        HttpProxyConfig dto = new HttpProxyConfig(false, "10.10.10.10", 1080, null, null);
        String[] curlArgs = clusterMgrService.getCurlArgs("/tmp/turbonomic-diags-_111.zip", dto);
        String[] expectedArgs = {"-F", "ufile=@/tmp/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL};
        // since proxy is not enabled, so proxy settings should be ignored
        assertArrayEquals(expectedArgs, curlArgs);
        final HttpProxyConfig dto2 = new HttpProxyConfig(true, "10.10.10.10", 1080, "user", null);
        curlArgs = clusterMgrService.getCurlArgs("/tmp/turbonomic-diags-_111.zip", dto2);
        String[] expectedArgsWithProxy = {"-F", "ufile=@/tmp/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL, "-x", "10.10.10.10:1080" };
        assertArrayEquals(expectedArgsWithProxy, curlArgs);
        final HttpProxyConfig dto3 = new HttpProxyConfig(true, "10.10.10.10", 1080, "user", "password");
        curlArgs = clusterMgrService.getCurlArgs("/tmp/turbonomic-diags-_111.zip", dto3);
        String[] expectedArgsWithSecureProxy = {"-F", "ufile=@/tmp/turbonomic-diags-_111.zip",
            UPLOAD_VMTURBO_COM_URL, "-x", "user:password@10.10.10.10:1080"};
        assertArrayEquals(expectedArgsWithSecureProxy, curlArgs);
    }

    @Test
    public void testInsertDiagsSummaryFileSuccess() throws IOException {
        ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
        clusterMgrService.insertDiagsSummaryFile(zipOutputStream, new StringBuilder());
        final ZipEntry zipEntry = new ZipEntry(ClusterMgrService.DIAGS_SUMMARY_SUCCESS_TXT);
        verify(zipOutputStream).putNextEntry(argThat(new MatchesZipEntry(zipEntry)));
        verify(zipOutputStream, never()).write(new byte[]{anyByte()}, anyInt(), anyInt());
        verify(zipOutputStream).closeEntry();
    }

    @Test
    public void testInsertDiagsSummaryFileFail() throws IOException {
        ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
        final StringBuilder errorMessages = new StringBuilder();
        errorMessages.append("clustermgr\n");
        clusterMgrService.insertDiagsSummaryFile(zipOutputStream, errorMessages);
        final ZipEntry zipEntry = new ZipEntry(ClusterMgrService.DIAGS_SUMMARY_FAIL_TXT);
        verify(zipOutputStream).putNextEntry(argThat(new MatchesZipEntry(zipEntry)));
        verify(zipOutputStream, times(1)).write(new byte[]{anyByte()}, anyInt(), anyInt());
        verify(zipOutputStream).closeEntry();
    }

    /**
     * Utility matcher so check whether the argument is a ZipEntry and that the
     * name matches the desired ZipEntry name.
     */
    private static class MatchesZipEntry extends ArgumentMatcher<ZipEntry> {

        private final ZipEntry zipEntry;

        MatchesZipEntry(final ZipEntry zipEntry) {
            this.zipEntry = zipEntry;
        }

        @Override
        public boolean matches(final Object argument) {
            if (!(argument instanceof ZipEntry)) {
                return false;
            }
            final ZipEntry entry = (ZipEntry)argument;
            return entry.getName().equals(zipEntry.getName());
        }
    }
}