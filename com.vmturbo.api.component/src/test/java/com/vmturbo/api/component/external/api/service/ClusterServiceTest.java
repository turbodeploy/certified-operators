package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.clustermgr.api.ClusterConfiguration;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.clustermgr.api.ComponentProperties;

/**
 * Test services for {@link ClusterService}.
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceTest {
    private static final String ASTERISKS = "*****";
    private static final String GROUP = "group";
    private static final String ARANGODB_PASS = "arangodbPass";
    private static final String USER_PASSWORD = "userPassword";
    private static final String SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    private static final String READONLY_PASSWORD = "readonlyPassword";
    private static final String DB_HOST = "dbHost";
    private static final String DB = "db";
    private static final String USERNAME = "username";

    @Mock
    private static ClusterMgrRestClient clusterManagerClient;

    @InjectMocks
    ClusterService serviceUnderTest;

    /**
     * Test fetching the cluster configuration. This includes checking fetch for masked
     * values for things like passwords.
     */
    @Test
    public void testGetClusterConfiguration() {
        final ClusterConfiguration clusterConfigurationDTO = new ClusterConfiguration();
        final ComponentProperties propertiesDTO = new ComponentProperties();
        // arrango password
        propertiesDTO.put(ARANGODB_PASS, "root");
        propertiesDTO.put(USER_PASSWORD, "root");
        propertiesDTO.put(SSL_KEYSTORE_PASSWORD, "root");
        propertiesDTO.put(READONLY_PASSWORD, "root");
        propertiesDTO.put(DB_HOST, DB);
        clusterConfigurationDTO.addComponentType(GROUP, propertiesDTO);
        Mockito.when(clusterManagerClient.getClusterConfiguration())
                .thenReturn(clusterConfigurationDTO);
        ClusterConfigurationDTO newClusterConfigurationDTO = serviceUnderTest.getClusterConfiguration();
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaultProperties().get(GROUP).get(ARANGODB_PASS));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaultProperties().get(GROUP).get(USER_PASSWORD));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaultProperties().get(GROUP).get(SSL_KEYSTORE_PASSWORD));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaultProperties().get(GROUP).get(READONLY_PASSWORD));
        assertEquals(DB, newClusterConfigurationDTO.getDefaultProperties().get(GROUP).get(DB_HOST));
    }

    /**
     * Test fetching the default properties for a component type. This includes checking for
     * masked values for things like passwords.
     */
    @Test
    public void testGetDefaultPropertiesForComponentType() {
        final ComponentProperties propertiesDTO = new ComponentProperties();
        // arrango password
        propertiesDTO.put(ARANGODB_PASS, "root");
        propertiesDTO.put(USER_PASSWORD, "root");
        propertiesDTO.put(SSL_KEYSTORE_PASSWORD, "root");
        propertiesDTO.put(READONLY_PASSWORD, "root");
        propertiesDTO.put(DB_HOST, DB);
        Mockito.when(clusterManagerClient.getComponentDefaultProperties(GROUP))
                .thenReturn(propertiesDTO);
        ComponentPropertiesDTO newPropertiesDTO = serviceUnderTest.getComponentDefaultProperties(GROUP);
        assertEquals(ASTERISKS, newPropertiesDTO.get(ARANGODB_PASS));
        assertEquals(ASTERISKS, newPropertiesDTO.get(USER_PASSWORD));
        assertEquals(ASTERISKS, newPropertiesDTO.get(SSL_KEYSTORE_PASSWORD));
        assertEquals(ASTERISKS, newPropertiesDTO.get(READONLY_PASSWORD));
        assertEquals(DB, newPropertiesDTO.get(DB_HOST));
    }

    /**
     * Test that fetching a default property from a component works, and that secret
     * values are masked.
     */
    @Test
    public void testGetComponentTypeProperty() {
        Mockito.when(clusterManagerClient.getComponentDefaultProperty(GROUP, ARANGODB_PASS))
                .thenReturn("root");
        Mockito.when(clusterManagerClient.getComponentDefaultProperty(GROUP, DB_HOST))
                .thenReturn(DB);
        String resultArrango = serviceUnderTest.getComponentDefaultProperty(GROUP, ARANGODB_PASS);
        assertEquals(ASTERISKS, resultArrango);
        String resultDB = serviceUnderTest.getComponentDefaultProperty(GROUP, DB_HOST);
        assertEquals(DB, resultDB);
    }

    /**
     * Test storing a complete cluster configuration.
     */
    @Test
    public void testSetClusterConfiguration() {
        // given
        final ClusterConfiguration originalClusterConfiguration = new ClusterConfiguration();
        final ComponentProperties propertiesDTO = new ComponentProperties();
        propertiesDTO.put(ARANGODB_PASS, "root");
        propertiesDTO.put(USER_PASSWORD, "root");
        propertiesDTO.put(DB_HOST, "DB");
        propertiesDTO.put(USERNAME, "tester");
        originalClusterConfiguration.addComponentType(GROUP, propertiesDTO);

        final ComponentPropertiesDTO newPropertiesDTO = new ComponentPropertiesDTO();
        newPropertiesDTO.put(ARANGODB_PASS, "*****"); //no change
        newPropertiesDTO.put(USER_PASSWORD, "newPassword"); //updated password
        newPropertiesDTO.put(DB_HOST, "newValue"); //updated other property
        newPropertiesDTO.put(USERNAME, "tester"); //no change
        final ClusterConfigurationDTO newClusterConfigurationDTO = new ClusterConfigurationDTO();
        newClusterConfigurationDTO.setDefaultProperties(GROUP, newPropertiesDTO);

        final  ClusterConfiguration mergedClusterConfiguration = new ClusterConfiguration();
        final ComponentProperties mergedPropertiesDTO = new ComponentProperties();
        mergedPropertiesDTO.put(ARANGODB_PASS, "root"); //should replace the "*****" with original password
        mergedPropertiesDTO.put(USER_PASSWORD, "newPassword"); //should update to new password
        mergedPropertiesDTO.put(DB_HOST, "newValue"); // should udpate to new value
        mergedPropertiesDTO.put(USERNAME, "tester"); // should not change
        mergedClusterConfiguration.addComponentType(GROUP, mergedPropertiesDTO);

        Mockito.when(clusterManagerClient.getClusterConfiguration())
                .thenReturn(originalClusterConfiguration);
        Mockito.when(clusterManagerClient.setClusterConfiguration(mergedClusterConfiguration))
                .thenReturn(mergedClusterConfiguration);
        // when
        serviceUnderTest.setClusterConfiguration(newClusterConfigurationDTO);

        // then
        Mockito.verify(clusterManagerClient, Mockito.times(1))
                .setClusterConfiguration(mergedClusterConfiguration);
        Mockito.verify(clusterManagerClient).getClusterConfiguration();
    }
}
