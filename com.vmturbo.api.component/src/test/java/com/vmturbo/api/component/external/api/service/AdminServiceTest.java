package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;

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

    @Configuration
    public static class ServiceTestConfig {

        @Bean
        public AdminService adminService() {
            return new AdminService(clusterService);
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
