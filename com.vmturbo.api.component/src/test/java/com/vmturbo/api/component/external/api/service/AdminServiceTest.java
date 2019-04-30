package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_ENABLED;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_HOST;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_PORT_NUMBER;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_NAME;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_PASSWORD;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Test services for the /admin endpoint.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AdminServiceTest.ServiceTestConfig.class)
public class AdminServiceTest {

    public static final String PUBLIC_VERSION_STRING = "public-version";
    public static final String TEST_VERSION_PROPERTY = "test-version";
    public static final String TEST_BUILD = "test-build";
    public static final String TEST_BUILD_TIME = "test-time";
    public static final String TEST_PACKAGE_NAME = "test-package-name";

    private static ClusterService clusterService = Mockito.mock(ClusterService.class);
    private static KeyValueStore keyValueStore = Mockito.mock(KeyValueStore.class);
    private AdminService adminService;
    private KeyValueStore keyValueStoreTest;
    private ClusterMgrRestClient clusterMgrClient;

    @Before
    public void setup(){
        clusterMgrClient = Mockito.mock(ClusterMgrRestClient.class);
        keyValueStoreTest = Mockito.mock(KeyValueStore.class);
        adminService = new AdminService(clusterService, keyValueStoreTest, clusterMgrClient);
    }

    @Autowired
    AdminService serviceUnderTest;

    @Test
    public void testGetVersionInfo() throws Exception {
        String instance1 = "inst1";
        String instance2 = "inst2";
        String instance1Type = "inst1Type";
        String instance2Type = "inst2Type";
        String instance1Version = "1.0.0";
        String instance2Version = "1.1.0";

        ClusterConfigurationDTO clusterConfigurationDTO = new ClusterConfigurationDTO();
        clusterConfigurationDTO.addComponentInstance(instance1, instance1Type, instance1Version, "node", new ComponentPropertiesDTO());
        clusterConfigurationDTO.addComponentInstance(instance2, instance2Type, instance2Version, "node", new ComponentPropertiesDTO());
        Mockito.when(clusterService.getClusterConfiguration())
                .thenReturn(clusterConfigurationDTO);

        // Arrange
        // Act
        ProductVersionDTO answer = serviceUnderTest.getVersionInfo(true);
        // Assert
        String versionInfo = answer.getVersionInfo();
        assertTrue(versionInfo.contains(instance1Type + ": " + instance1Version));
        assertTrue(versionInfo.contains(instance2Type + ": " + instance2Version));
    }

    @Test
    public void testSetProxyConfigInsureProxy() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setPortNumber("3306");
        dto.setIsProxyEnabled(true);
        adminService.setProxyConfig(dto);
        verify(keyValueStoreTest).put(eq(PROXY_ENABLED), eq("true"));
        verify(keyValueStoreTest).put(eq(PROXY_HOST), eq("10.10.10.1"));
        verify(keyValueStoreTest).put(eq(PROXY_PORT_NUMBER), eq("3306"));
        verify(keyValueStoreTest, never()).put(eq(PROXY_USER_NAME), anyString());
        verify(keyValueStoreTest, never()).put(eq(PROXY_USER_PASSWORD), anyString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetProxyConfigSureProxyWithAsterisksPassword() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setPortNumber("3306");
        dto.setIsProxyEnabled(true);
        dto.setUserName("user");
        dto.setPassword(ClusterService.ASTERISKS);
        serviceUnderTest.setProxyConfig(dto);
    }

    @Test(expected = InvalidOperationException.class)
    public void testSetProxyConfigSureProxyMissingPort() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setIsProxyEnabled(true);
        serviceUnderTest.setProxyConfig(dto);
    }

    @Test
    public void testSetProxyConfigSureProxy() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setPortNumber("3306");
        dto.setIsProxyEnabled(true);
        dto.setUserName("user");
        dto.setPassword(ClusterService.ASTERISKS);
        adminService.storeProxyConfig(dto, true, true);
        verify(keyValueStoreTest).put(eq(PROXY_ENABLED), eq("true"));
        verify(keyValueStoreTest).put(eq(PROXY_HOST), eq("10.10.10.1"));
        verify(keyValueStoreTest).put(eq(PROXY_PORT_NUMBER), eq("3306"));
        verify(keyValueStoreTest).put(eq(PROXY_USER_NAME), eq("user"));
        verify(keyValueStoreTest).put(eq(PROXY_USER_PASSWORD), eq(ClusterService.ASTERISKS));
    }

    @Configuration
    public static class ServiceTestConfig {

        @Bean
        public AdminService adminService() {
            final ClusterMgrRestClient clusterMgrClient = Mockito.mock(ClusterMgrRestClient.class);
            return new AdminService(clusterService, keyValueStore, clusterMgrClient);
        }

        @Bean
        public static PropertySourcesPlaceholderConfigurer properties() throws Exception {
            final PropertySourcesPlaceholderConfigurer propertiesConfigureer
                    = new PropertySourcesPlaceholderConfigurer();

            Properties properties = new Properties();
            properties.setProperty("publicVersionString", PUBLIC_VERSION_STRING);
            properties.setProperty("build-number.version", TEST_VERSION_PROPERTY);
            properties.setProperty("build-number.build", TEST_BUILD);
            properties.setProperty("build-number.time", TEST_BUILD_TIME);
            properties.setProperty("build-number.package", TEST_PACKAGE_NAME);

            propertiesConfigureer.setProperties(properties);
            return propertiesConfigureer;
        }
    }

}
