package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;

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

    private static ClusterMgrClient clusterManagerClient = Mockito.mock(ClusterMgrClient.class);

    @Autowired
    ClusterService serviceUnderTest;

    @Test
    public void testGetClusterConfiguration() {
        ClusterConfigurationDTO clusterConfigurationDTO = new ClusterConfigurationDTO();
        ComponentPropertiesDTO propertiesDTO = new ComponentPropertiesDTO();
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
        ComponentPropertiesDTO propertiesDTO = new ComponentPropertiesDTO();
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

    @Configuration
    public static class ServiceTestConfig {
        @Bean
        public ClusterService adminService() {
            return new ClusterService(clusterManagerClient);
        }
    }

}
