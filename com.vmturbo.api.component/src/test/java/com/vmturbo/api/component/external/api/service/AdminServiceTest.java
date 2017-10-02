package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.api.dto.ProductVersionDTO;

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

    @Autowired
    AdminService serviceUnderTest;

    @Test
    public void testGetVersionInfo() throws Exception {
        // Arrange
        // Act
        ProductVersionDTO answer = serviceUnderTest.getVersionInfo();
        // Assert
        assertThat(answer.getVersionInfo(), containsString(TEST_VERSION_PROPERTY));
        assertThat(answer.getVersionInfo(), containsString(TEST_BUILD));
        assertThat(answer.getVersionInfo(), containsString(TEST_BUILD_TIME));
        // note that the package name is not currently used in the message string and so isn't
        //   tested here.
    }

    @Configuration
    public static class ServiceTestConfig {

        @Bean
        public AdminService adminService() {
            return new AdminService();
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