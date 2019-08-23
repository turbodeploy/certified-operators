package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.clustermgr.api.ClusterConfiguration;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.clustermgr.api.ComponentProperties;

/**
 * Test services for {@link ClusterService}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ClusterServiceTest.ServiceTestConfig.class)
public class ClusterServiceTest {
    public static final String ASTERISKS = "*****";
    public static final String GROUP = "group";
    public static final String ARANGODB_PASS = "arangodbPass";
    public static final String USER_PASSWORD = "userPassword";
    public static final String SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    public static final String READONLY_PASSWORD = "readonlyPassword";
    public static final String DB_HOST = "dbHost";
    public static final String DB = "db";
    public static final String USERNAME = "username";

    private static ClusterMgrRestClient clusterManagerClient = Mockito.mock(ClusterMgrRestClient.class);

    @Autowired
    ClusterService serviceUnderTest;

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
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaults().get(GROUP).get(ARANGODB_PASS));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaults().get(GROUP).get(USER_PASSWORD));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaults().get(GROUP).get(SSL_KEYSTORE_PASSWORD));
        assertEquals(ASTERISKS, newClusterConfigurationDTO.getDefaults().get(GROUP).get(READONLY_PASSWORD));
        assertEquals(DB, newClusterConfigurationDTO.getDefaults().get(GROUP).get(DB_HOST));
    }

    @Test
    public void testGetDefaultPropertiesForComponentType() {
        final ComponentProperties propertiesDTO = new ComponentProperties();
        // arrango password
        propertiesDTO.put(ARANGODB_PASS, "root");
        propertiesDTO.put(USER_PASSWORD, "root");
        propertiesDTO.put(SSL_KEYSTORE_PASSWORD, "root");
        propertiesDTO.put(READONLY_PASSWORD, "root");
        propertiesDTO.put(DB_HOST, DB);
        Mockito.when(clusterManagerClient.getDefaultPropertiesForComponentType(GROUP))
                .thenReturn(propertiesDTO);
        ComponentPropertiesDTO newPropertiesDTO = serviceUnderTest.getDefaultPropertiesForComponentType(GROUP);
        assertEquals(ASTERISKS, newPropertiesDTO.get(ARANGODB_PASS));
        assertEquals(ASTERISKS, newPropertiesDTO.get(USER_PASSWORD));
        assertEquals(ASTERISKS, newPropertiesDTO.get(SSL_KEYSTORE_PASSWORD));
        assertEquals(ASTERISKS, newPropertiesDTO.get(READONLY_PASSWORD));
        assertEquals(DB, newPropertiesDTO.get(DB_HOST));
    }

    @Test
    public void testGetComponentTypeProperty() {
        Mockito.when(clusterManagerClient.getComponentTypeProperty(GROUP, ARANGODB_PASS))
                .thenReturn("root");
        Mockito.when(clusterManagerClient.getComponentTypeProperty(GROUP, DB_HOST))
                .thenReturn(DB);
        String resultArrango = serviceUnderTest.getComponentTypeProperty(GROUP, ARANGODB_PASS);
        assertEquals(ASTERISKS, resultArrango);
        String resultDB = serviceUnderTest.getComponentTypeProperty(GROUP, DB_HOST);
        assertEquals(DB, resultDB);
    }

    @Test
    public void testSetClusterConfiguration() {
        // given
        final ClusterConfiguration originalClusterConfigurationDTO = new ClusterConfiguration();
        final ComponentProperties propertiesDTO = new ComponentProperties();
        propertiesDTO.put(ARANGODB_PASS, "root");
        propertiesDTO.put(USER_PASSWORD, "root");
        propertiesDTO.put(DB_HOST, "DB");
        propertiesDTO.put(USERNAME, "tester");
        originalClusterConfigurationDTO.addComponentType(GROUP, propertiesDTO);

        ClusterConfigurationDTO newClusterConfigurationDTO = new ClusterConfigurationDTO();
        ComponentPropertiesDTO newPropertiesDTO = new ComponentPropertiesDTO();
        newPropertiesDTO.put(ARANGODB_PASS, "*****"); //no change
        newPropertiesDTO.put(USER_PASSWORD, "newPassword"); //updated password
        newPropertiesDTO.put(DB_HOST, "newValue"); //updated other property
        newPropertiesDTO.put(USERNAME, "tester"); //no change
        newClusterConfigurationDTO.addComponentType(GROUP, newPropertiesDTO);

        final  ClusterConfiguration mergedClusterConfigurationDTO = new ClusterConfiguration();
        final ComponentProperties mergedPropertiesDTO = new ComponentProperties();
        mergedPropertiesDTO.put(ARANGODB_PASS, "root"); //should replace the "*****" with original password
        mergedPropertiesDTO.put(USER_PASSWORD, "newPassword"); //should update to new password
        mergedPropertiesDTO.put(DB_HOST, "newValue"); // should udpate to new value
        mergedPropertiesDTO.put(USERNAME, "tester"); // should not change
        mergedClusterConfigurationDTO.addComponentType(GROUP, mergedPropertiesDTO);

        Mockito.when(clusterManagerClient.getClusterConfiguration())
                .thenReturn(originalClusterConfigurationDTO);
        Mockito.when(clusterManagerClient.setClusterConfiguration(mergedClusterConfigurationDTO))
                .thenReturn(mergedClusterConfigurationDTO);
        // when
        serviceUnderTest.setClusterConfiguration(newClusterConfigurationDTO);

        // then
        Mockito.verify(clusterManagerClient, Mockito.times(1))
                .setClusterConfiguration(mergedClusterConfigurationDTO);
    }

    @Configuration
    public static class ServiceTestConfig {
        @Bean
        public ClusterService adminService() {
            return new ClusterService(clusterManagerClient);
        }
    }

}
