package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_ENABLED;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_HOST;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_PORT_NUMBER;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_NAME;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController.LogConfigurationServiceResponse;
import com.vmturbo.components.common.utils.LoggingUtils;
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
    private static final String API_COMPONENT = "api";
    private static final String AUTH_COMPONENT = "auth";
    private static final String MARKET_COMPONENT = "market";

    private static ClusterService clusterService = Mockito.mock(ClusterService.class);
    private static KeyValueStore keyValueStore = Mockito.mock(KeyValueStore.class);
    private static RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    private AdminService adminService;
    private KeyValueStore keyValueStoreTest;
    private ClusterMgrRestClient clusterMgrClient;

    @Before
    public void setup(){
        clusterMgrClient = Mockito.mock(ClusterMgrRestClient.class);
        keyValueStoreTest = Mockito.mock(KeyValueStore.class);
        adminService = new AdminService(clusterService, keyValueStoreTest, clusterMgrClient, restTemplate);
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

    @Test
    public void testGetLoggingLevels() throws Exception {
        Mockito.when(clusterService.getKnownComponents()).thenReturn(
            ImmutableSet.of(API_COMPONENT, AUTH_COMPONENT, MARKET_COMPONENT));
        // mock logging level for api and other components
        Configurator.setRootLevel(Level.WARN);
        mockGetLogLevelForComponent(AUTH_COMPONENT, LogLevel.DEBUG);
        mockGetLogLevelForComponent(MARKET_COMPONENT, LogLevel.TRACE);

        LoggingApiDTO loggingApiDTO = serviceUnderTest.getLoggingLevels();
        Map<String, LoggingLevel> loggingLevel = loggingApiDTO.getComponentLoggingLevel();

        assertEquals(3, loggingLevel.size());
        assertEquals(LoggingLevel.WARN, loggingLevel.get(API_COMPONENT));
        assertEquals(LoggingLevel.DEBUG, loggingLevel.get(AUTH_COMPONENT));
        assertEquals(LoggingLevel.TRACE, loggingLevel.get(MARKET_COMPONENT));

        // set api logging level back to INFO
        Configurator.setRootLevel(Level.INFO);
    }

    @Test
    public void testSetLoggingLevels() throws Exception {
        LoggingApiDTO loggingApiDTO = new LoggingApiDTO();
        loggingApiDTO.setComponentLoggingLevel(ImmutableMap.of(
            API_COMPONENT, LoggingLevel.WARN,
            AUTH_COMPONENT, LoggingLevel.DEBUG,
            MARKET_COMPONENT, LoggingLevel.TRACE
        ));

        mockSetLogLevelForComponent(AUTH_COMPONENT, LoggingLevel.DEBUG);
        mockSetLogLevelForComponent(MARKET_COMPONENT, LoggingLevel.TRACE);

        LoggingApiDTO newloggingApiDTO = serviceUnderTest.setLoggingLevelForGivenComponent(loggingApiDTO);
        Map<String, LoggingLevel> loggingLevel = newloggingApiDTO.getComponentLoggingLevel();

        assertEquals(3, loggingLevel.size());
        assertEquals(LoggingLevel.WARN, loggingLevel.get("api"));
        assertEquals(LoggingLevel.DEBUG, loggingLevel.get("auth"));
        assertEquals(LoggingLevel.TRACE, loggingLevel.get("market"));

        // set api logging level back to INFO
        Configurator.setRootLevel(Level.INFO);
    }

    /**
     * Mock getting logging level for the given component using the provided LogLevel.
     */
    private void mockGetLogLevelForComponent(String component, LogLevel logLevel) throws Exception {
        Constructor<LogConfigurationServiceResponse> constructor =
            LogConfigurationServiceResponse.class.getDeclaredConstructor(Object.class, String.class);
        constructor.setAccessible(true);
        LogConfigurationServiceResponse response = constructor.newInstance(
            GetLogLevelsResponse.newBuilder()
                .putLogLevels(LogManager.ROOT_LOGGER_NAME, logLevel).build(), null);

        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(component)
            .port(8080)
            .path("/LogConfigurationService/getLogLevels");

        when(restTemplate.postForObject(builder.build().toString(),
            new HttpEntity<>(GetLogLevelsRequest.getDefaultInstance()),
            LogConfigurationServiceResponse.class)).thenReturn(response);
    }

    /**
     * Mock setting logging level for the given component using the provided LoggingLevel.
     */
    private void mockSetLogLevelForComponent(String component, LoggingLevel logLevel) throws Exception {
        Constructor<LogConfigurationServiceResponse> constructor =
            LogConfigurationServiceResponse.class.getDeclaredConstructor(Object.class, String.class);
        constructor.setAccessible(true);
        LogConfigurationServiceResponse response = constructor.newInstance(
            SetLogLevelsResponse.getDefaultInstance(), null);

        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(component)
            .port(8080)
            .path("/LogConfigurationService/setLogLevels");

        when(restTemplate.postForObject(builder.build().toString(),
            new HttpEntity<>(SetLogLevelsRequest.newBuilder()
                .putLogLevels(LogManager.ROOT_LOGGER_NAME,
                    LoggingUtils.apiLogLevelToProtoLogLevel(logLevel)).build()),
            LogConfigurationServiceResponse.class)).thenReturn(response);
    }

    @Configuration
    public static class ServiceTestConfig {

        @Bean
        public AdminService adminService() {
            final ClusterMgrRestClient clusterMgrClient = Mockito.mock(ClusterMgrRestClient.class);
            return new AdminService(clusterService, keyValueStore, clusterMgrClient, restTemplate);
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
            properties.setProperty("apiHost", "api");
            properties.setProperty("serverHttpPort", "8080");

            propertiesConfigureer.setProperties(properties);
            return propertiesConfigureer;
        }
    }

}
