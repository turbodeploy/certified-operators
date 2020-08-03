package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_ENABLED;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_HOST;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_PORT_NUMBER;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_NAME;
import static com.vmturbo.api.component.external.api.service.AdminService.PROXY_USER_PASSWORD;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.http.HttpEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.component.external.api.mapper.LoggingMapper;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.ProductCapabilityDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.enums.DeploymentMode;
import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController.LogConfigurationServiceResponse;
import com.vmturbo.components.common.logging.LogConfigurationService;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Test services for the /admin endpoint.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AdminServiceTest.ServiceTestConfig.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class AdminServiceTest {

    private static final String PUBLIC_VERSION_STRING = "public-version";
    private static final String TEST_VERSION_PROPERTY = "test-version";
    private static final String TEST_BUILD = "test-build";
    private static final String TEST_BUILD_TIME = "test-time";
    private static final String TEST_PACKAGE_NAME = "test-package-name";
    private static final String API_COMPONENT = "api";
    private static final String AUTH_COMPONENT = "auth";
    private static final String MARKET_COMPONENT = "market";

    private static final String GIT_PROP_BRANCH = "testBranch";
    private static final String GIT_BUILD_TIME = "123";
    private static final String GIT_VERSION = "MyVersion";
    private static final String GIT_COMMIT_ID = "someCommitHash!";
    private static final String GIT_SHORT_COMMIT_ID = "shortCommitHash";
    private static final boolean GIT_DIRTY = true;

    private static ClusterService clusterService = Mockito.mock(ClusterService.class);
    private static RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    private static SettingsService settingsService = Mockito.mock(SettingsService.class);

    @Autowired
    private KeyValueStore keyValueStoreTest;

    @Autowired
    private ClusterMgrRestClient clusterMgrClient;

    @Autowired
    private ApiWebsocketHandler apiWebsocketHandler;

    @Autowired
    private AdminService serviceUnderTest;

    /**
     * Test that the {@link ProductVersionDTO} returned by the
     * {@link AdminService#getVersionInfo(boolean)} call contains the right information.
     */
    @Test
    public void testGetVersionInfo() {
        final String instance1 = "inst1";
        final String instance2 = "inst2";
        final String instance1Type = "inst1Type";
        final String instance2Type = "inst2Type";
        final String instance1Version = "1.0.0";
        final String instance2Version = "1.1.0";

        final ClusterConfigurationDTO clusterConfigurationDTO = new ClusterConfigurationDTO();
        final ComponentPropertiesDTO instance1Defaults = new ComponentPropertiesDTO();
        instance1Defaults.put("component.version", instance1Version);
        final ComponentPropertiesDTO instance2Defaults = new ComponentPropertiesDTO();
        instance2Defaults.put("component.version", instance2Version);
        clusterConfigurationDTO.setDefaultProperties(instance1Type, instance1Defaults);
        clusterConfigurationDTO.setDefaultProperties(instance2Type, instance2Defaults);
        clusterConfigurationDTO.addComponentInstance(instance1, instance1Type, instance1Version,
            new ComponentPropertiesDTO());
        clusterConfigurationDTO.addComponentInstance(instance2, instance2Type, instance2Version,
            new ComponentPropertiesDTO());
        Mockito.when(clusterService.getClusterConfiguration())
                .thenReturn(clusterConfigurationDTO);

        // Arrange
        // Act
        ProductVersionDTO answer = serviceUnderTest.getVersionInfo(true);
        // Assert
        String versionInfo = answer.getVersionInfo();
        assertTrue(versionInfo.contains(instance1Type + ": " + instance1Version));
        assertTrue(versionInfo.contains(instance2Type + ": " + instance2Version));

        // Assert similarity to the git.properties file.
        assertThat(answer.getVersion(), is(GIT_VERSION));
        assertThat(answer.getBranch(), is(GIT_PROP_BRANCH));
        assertThat(answer.getBuild(), is(GIT_BUILD_TIME));
        assertThat(answer.getCommit(), is(GIT_SHORT_COMMIT_ID));
        assertThat(answer.hasCodeChanges(), is(GIT_DIRTY));
        assertThat(answer.getGitDescription(), is(GIT_COMMIT_ID + " dirty"));
    }

    /**
     * Test {@link AdminService#getVersionInfo(boolean)} with default {@link DeploymentMode}.
     *
     * <p>Server congfiguration sets reporting to true</p>
     * @throws Exception if an error occurs
     */
    @Test
    public void testGetProductCapabilities() throws Exception {
        //WHEN
        ProductCapabilityDTO dto = serviceUnderTest.getProductCapabilities();

        //THEN
        assertEquals(DeploymentMode.SERVER, dto.getDeploymentMode());
        assertFalse(dto.isReportingEnabled());
    }

    /**
     * Test {@link AdminService#getVersionInfo(boolean)} with SaaS {@link DeploymentMode}.
     *
     * @throws Exception if an error occurs
     */
    @Test
    public void testGetProductCapabilitiesWithSaasSetProductType() throws Exception {
        //GIVEN
        AdminService adminService = new AdminService(Mockito.mock(ClusterService.class),
                Mockito.mock(KeyValueStore.class),
                Mockito.mock(ClusterMgrRestClient.class),
                Mockito.mock(RestTemplate.class),
                Mockito.mock(ApiWebsocketHandler.class),
                Mockito.mock(BuildProperties.class),
                DeploymentMode.SAAS,
                false,
                Mockito.mock(SettingsService.class));
        //WHEN
        ProductCapabilityDTO dto = adminService.getProductCapabilities();

        //THEN
        assertEquals(DeploymentMode.SAAS, dto.getDeploymentMode());
        assertFalse(dto.isReportingEnabled());
    }

    /**
     * Test {@link AdminService#getVersionInfo(boolean)} with uiReportEnabled to true {@link DeploymentMode}.
     *
     * @throws Exception if an error occurs
     */
    @Test
    public void testGetProductCapabilitiesWithUIReportEnabled() throws Exception {
        //GIVEN
        AdminService adminService = new AdminService(Mockito.mock(ClusterService.class),
                Mockito.mock(KeyValueStore.class),
                Mockito.mock(ClusterMgrRestClient.class),
                Mockito.mock(RestTemplate.class),
                Mockito.mock(ApiWebsocketHandler.class),
                Mockito.mock(BuildProperties.class),
                DeploymentMode.SAAS,
                true,
                Mockito.mock(SettingsService.class));
        //WHEN
        ProductCapabilityDTO dto = adminService.getProductCapabilities();

        //THEN
        assertTrue(dto.isReportingEnabled());
    }

    /**
     * Test that {@link AdminService#invokeSchedulerToExportDiags()} calls cluster manager to
     * export diagnostics, and broadcasts a notification if the export succeeds.
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testExportDialDataSucceed() throws Exception {
        when(clusterMgrClient.exportComponentDiagnostics(any())).thenReturn(true);
        Future<Boolean> future = serviceUnderTest.invokeSchedulerToExportDiags();
        assertTrue(future.get(1000L, TimeUnit.SECONDS));
        verify(apiWebsocketHandler).broadcastDiagsExportNotification(AdminService.EXPORTED_DIAGNOSTICS_SUCCEED);
    }

    /**
     * Test that {@link AdminService#invokeSchedulerToExportDiags()} calls cluster manager to
     * export diagnostics, and broadcasts a notification if the export fails.
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testExportDiagDataFailed() throws Exception {
        when(clusterMgrClient.exportComponentDiagnostics(any())).thenReturn(false);
        Future<Boolean> future = serviceUnderTest.invokeSchedulerToExportDiags();
        assertFalse(future.get(1000L, TimeUnit.SECONDS));
        verify(apiWebsocketHandler).broadcastDiagsExportNotification(AdminService.FAILED_TO_EXPORT_DIAGNOSTICS_FAILED);
    }

    /**
     * Test that {@link AdminService#invokeSchedulerToExportDiags()} calls cluster manager to
     * export diagnostics, and broadcasts a notification if the export fails with a runtime
     * exception (e.g. clustermgr is down).
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testExportDiagDataFailedWithRuntimeException() throws Exception {
        when(clusterMgrClient.exportComponentDiagnostics(any())).thenThrow(new RuntimeException());
        Future<Boolean> future = serviceUnderTest.invokeSchedulerToExportDiags();
        assertFalse(future.get(1000L, TimeUnit.SECONDS));
        verify(apiWebsocketHandler).broadcastDiagsExportNotification(AdminService.FAILED_TO_EXPORT_DIAGNOSTICS_FAILED);
    }

    /**
     * Test that {@link AdminService#setProxyConfig(HttpProxyDTO)} puts the proxy information
     * for an insecure proxy into the key value store.
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testSetProxyConfigInsecureProxy() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setProxyPortNumber(3306);
        dto.setIsProxyEnabled(true);
        serviceUnderTest.setProxyConfig(dto);
        verify(keyValueStoreTest).put(eq(PROXY_ENABLED), eq("true"));
        verify(keyValueStoreTest).put(eq(PROXY_HOST), eq("10.10.10.1"));
        verify(keyValueStoreTest).put(eq(PROXY_PORT_NUMBER), eq("3306"));
        verify(keyValueStoreTest, never()).put(eq(PROXY_USER_NAME), anyString());
        verify(keyValueStoreTest, never()).put(eq(PROXY_USER_PASSWORD), anyString());
    }

    /**
     * Test disable proxy will clean up proxy information in key/value store.
     *
     * @throws Exception If anything is wrong.
     */
    @Test
    public void testDisableProxyConfig() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setProxyPortNumber(3306);
        dto.setIsProxyEnabled(false);
        serviceUnderTest.setProxyConfig(dto);
        verify(keyValueStoreTest).removeKey(eq(PROXY_ENABLED));
        verify(keyValueStoreTest).removeKey(eq(PROXY_HOST));
        verify(keyValueStoreTest).removeKey(eq(PROXY_PORT_NUMBER));
        verify(keyValueStoreTest).removeKey(eq(PROXY_USER_NAME));
        verify(keyValueStoreTest).removeKey(eq(PROXY_USER_PASSWORD));
    }

    /**
     * Test that {@link AdminService#setProxyConfig(HttpProxyDTO)} throws an exception when given
     * an invalid password.
     *
     * @throws Exception If anything is wrong.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetProxyConfigSureProxyWithAsterisksPassword() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setProxyPortNumber(3306);
        dto.setIsProxyEnabled(true);
        dto.setUserName("user");
        dto.setPassword(ClusterService.ASTERISKS);
        serviceUnderTest.setProxyConfig(dto);
    }

    /**
     * Test that {@link AdminService#setProxyConfig(HttpProxyDTO)} throws an exception when the
     * proxy is missing a port.
     *
     * @throws Exception If anything is wrong.
     */
    @Test(expected = InvalidOperationException.class)
    public void testSetProxyConfigSureProxyMissingPort() throws Exception {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setIsProxyEnabled(true);
        serviceUnderTest.setProxyConfig(dto);
    }

    /**
     * Test that {@link AdminService#storeProxyConfig(HttpProxyDTO, boolean, boolean)} properly
     * stores secure proxy information in the key-value store.
     */
    @Test
    public void testSetProxyConfigSecureProxy() {
        HttpProxyDTO dto = new HttpProxyDTO();
        dto.setProxyHost("10.10.10.1");
        dto.setProxyPortNumber(3306);
        dto.setIsProxyEnabled(true);
        dto.setUserName("user");
        dto.setPassword(ClusterService.ASTERISKS);
        serviceUnderTest.storeProxyConfig(dto, true, true);
        verify(keyValueStoreTest).put(eq(PROXY_ENABLED), eq("true"));
        verify(keyValueStoreTest).put(eq(PROXY_HOST), eq("10.10.10.1"));
        verify(keyValueStoreTest).put(eq(PROXY_PORT_NUMBER), eq("3306"));
        verify(keyValueStoreTest).put(eq(PROXY_USER_NAME), eq("user"));
        verify(keyValueStoreTest).put(eq(PROXY_USER_PASSWORD), eq(ClusterService.ASTERISKS));
    }

    /**
     * Test getting logging levels from the service.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetLoggingLevels() throws Exception {
        Mockito.when(clusterService.getKnownComponents()).thenReturn(
            ImmutableSet.of(API_COMPONENT, AUTH_COMPONENT, MARKET_COMPONENT));
        // mock logging level for api and other components
        Configurator.setLevel(LogConfigurationService.TURBO_PACKAGE_NAME, Level.WARN);
        mockGetLogLevelForComponent(AUTH_COMPONENT, LogLevel.DEBUG);
        mockGetLogLevelForComponent(MARKET_COMPONENT, LogLevel.TRACE);

        LoggingApiDTO loggingApiDTO = serviceUnderTest.getLoggingLevels();
        Map<String, LoggingLevel> loggingLevel = loggingApiDTO.getComponentLoggingLevel();

        assertEquals(3, loggingLevel.size());
        assertEquals(LoggingLevel.WARN, loggingLevel.get(API_COMPONENT));
        assertEquals(LoggingLevel.DEBUG, loggingLevel.get(AUTH_COMPONENT));
        assertEquals(LoggingLevel.TRACE, loggingLevel.get(MARKET_COMPONENT));

        // set api logging level back to INFO
        Configurator.setLevel(LogConfigurationService.TURBO_PACKAGE_NAME, Level.INFO);
    }

    /**
     * Test that setting logging levels in the service propagates the calls to the requested
     * components.
     *
     * @throws Exception If anything goes wrong.
     */
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
        Configurator.setLevel(LogConfigurationService.TURBO_PACKAGE_NAME, Level.INFO);
    }

    /**
     * Mock getting logging level for the given component using the provided LogLevel.
     *
     * @param component The component name.
     * @param logLevel The log level.
     * @throws Exception If anything goes wrong.
     */
    private void mockGetLogLevelForComponent(String component, LogLevel logLevel) throws Exception {
        Constructor<LogConfigurationServiceResponse> constructor =
            LogConfigurationServiceResponse.class.getDeclaredConstructor(Object.class, String.class);
        constructor.setAccessible(true);
        LogConfigurationServiceResponse response = constructor.newInstance(
            GetLogLevelsResponse.newBuilder()
                .putLogLevels(LogConfigurationService.TURBO_PACKAGE_NAME, logLevel).build(), null);

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
     *
     * @param component The component name.
     * @param logLevel The log level.
     * @throws Exception If anything goes wrong.
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
                .putLogLevels(LogConfigurationService.TURBO_PACKAGE_NAME,
                    LoggingMapper.apiLogLevelToProtoLogLevel(logLevel)).build()),
            LogConfigurationServiceResponse.class)).thenReturn(response);
    }

    /**
     * Configuration used to set up the testing harness.
     */
    @Configuration
    public static class ServiceTestConfig {

        /**
         * Mock {@link ClusterMgrRestClient}.
         *
         * @return The mock object.
         */
        @Bean
        public ClusterMgrRestClient clusterMgrClient() {
            return Mockito.mock(ClusterMgrRestClient.class);
        }

        /**
         * Mock {@link KeyValueStore}.
         *
         * @return The mock object.
         */
        @Bean
        public KeyValueStore keyValueStore() {
            KeyValueStore keyValueStore = Mockito.mock(KeyValueStore.class);
            when(keyValueStore.get(anyString())).thenReturn(Optional.empty());
            return keyValueStore;
        }

        /**
         * Mock {@link ApiWebsocketHandler}.
         *
         * @return The mock object.
         */
        @Bean
        public ApiWebsocketHandler apiWebsocketHandler() {
            return Mockito.mock(ApiWebsocketHandler.class);
        }

        /**
         * Mock {@link AdminService}.
         *
         * @return The mock object.
         */
        @Bean
        public AdminService adminService() {
            return new AdminService(clusterService, keyValueStore(),
                clusterMgrClient(), restTemplate, apiWebsocketHandler(),
                buildProperties(), DeploymentMode.SERVER, false, settingsService);
        }

        /**
         * The {@link PropertySourcesPlaceholderConfigurer}.
         *
         * @return The object.
         */
        @Bean
        public static PropertySourcesPlaceholderConfigurer properties() {
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

        /**
         * Mock {@link BuildProperties}.
         *
         * @return The mock object.
         */
        @Bean
        public BuildProperties buildProperties() {
            BuildProperties buildProperties = Mockito.mock(BuildProperties.class);
            when(buildProperties.getShortCommitId()).thenReturn(GIT_SHORT_COMMIT_ID);
            when(buildProperties.getVersion()).thenReturn(GIT_VERSION);
            when(buildProperties.getBranch()).thenReturn(GIT_PROP_BRANCH);
            when(buildProperties.getCommitId()).thenReturn(GIT_COMMIT_ID);
            when(buildProperties.getBuildTime()).thenReturn(GIT_BUILD_TIME);
            when(buildProperties.isDirty()).thenReturn(GIT_DIRTY);
            return buildProperties;
        }
    }

}
