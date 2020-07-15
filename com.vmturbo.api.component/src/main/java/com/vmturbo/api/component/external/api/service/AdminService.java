package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.TelemetryEnabled;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.TelemetryTermsAccepted;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.api.ExportNotificationDTO.ExportNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportStatusNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportStatusNotification.ExportStatus;
import com.vmturbo.api.component.external.api.mapper.LoggingMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.ProductCapabilityDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.admin.SystemStatusApiDTO;
import com.vmturbo.api.dto.admin.TelemetryDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.enums.DeploymentMode;
import com.vmturbo.api.enums.ConfigurationType;
import com.vmturbo.api.enums.DeploymentMode;
import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.IAdminService;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.clustermgr.api.HttpProxyConfig;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController.LogConfigurationServiceResponse;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.logging.LogConfigurationService;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.components.common.utils.LoggingUtils;
import com.vmturbo.components.common.utils.Strings;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * implement the services behind the "/admin" endpoint. This includes logging, proxy, diagnostics,
 * system status, version info, etc.
 */
public class AdminService implements IAdminService {

    /**
     * XL only uses market 2.
     */
    private static final int MARKET_VERSION = 2;

    private static final String VERSION_INFO_HEADER = "Turbonomic Operations Manager {0} (Build {1}) {2}\n\n";

    private static final String UPDATES_NOT_IMPLEMENTED = "<not implemented>";

    // Following strings match user inputs.
    // Is proxy enabled?
    @VisibleForTesting
    static final String PROXY_ENABLED = "proxyEnabled";

    // Proxy server user name.
    @VisibleForTesting
    static final String PROXY_USER_NAME = "proxyUserName";

    // Proxy server password.
    @VisibleForTesting
    static final String PROXY_USER_PASSWORD = "proxyUserPassword";

    // Proxy server host name.
    @VisibleForTesting
    static final String PROXY_HOST = "proxyHost";

    // Proxy server port number.
    @VisibleForTesting
    static final String PROXY_PORT_NUMBER = "proxyPortNumber";

    @VisibleForTesting
    static final ExportNotification EXPORTED_DIAGNOSTICS_SUCCEED = ExportNotification.newBuilder()
        .setStatusNotification(ExportStatusNotification.newBuilder()
            .setStatus(ExportStatus.SUCCEEDED)
            .setDescription("Exported diagnostics data").build())
        .build();

    @VisibleForTesting
    static final ExportNotification FAILED_TO_EXPORT_DIAGNOSTICS_FAILED = ExportNotification.newBuilder()
        .setStatusNotification(ExportStatusNotification.newBuilder()
            .setStatus(ExportStatus.FAILED)
            .setDescription("Failed to export diagnostics data").build())
        .build();

    private static final HttpProxyDTO EMPTY_PROXY_DTO = new HttpProxyDTO();

    // use single thread to ensure only one export diagnostics at a time.
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final KeyValueStore keyValueStore;

    private final ApiWebsocketHandler apiWebsocketHandler;

    @Value("${publicVersionString}")
    private String publicVersionString;

    @Value("${build-number.build}")
    private String buildNumber;

    @Value("${build-number.time}")
    private String buildTime;

    @Value("${build-number.package}")
    private String buildPackage;

    @Value("${apiHost}")
    private String apiHost;

    @Value("${serverHttpPort}")
    private Integer httpPort;

    private static final Gson GSON = ComponentGsonFactory.createGson();

    private final ClusterService clusterService;

    private final Logger logger = LogManager.getLogger();

    private ClusterMgrRestClient clusterMgrApi;

    private RestTemplate restTemplate;

    private final BuildProperties buildProperties;

    private DeploymentMode deploymentMode;

    private final boolean enableReporting;

    private final SettingsService settingsService;

    AdminService(@Nonnull final ClusterService clusterService,
                 @Nonnull final KeyValueStore keyValueStore,
                 @Nonnull final ClusterMgrRestClient clusterMgrApi,
                 @Nonnull final RestTemplate restTemplate,
                 @Nonnull final ApiWebsocketHandler apiWebsocketHandler,
                 @Nonnull final BuildProperties buildProperties,
                 @Nonnull final DeploymentMode deploymentMode,
                 @Nonnull final boolean enableReporting,
                 @Nonnull final SettingsService settingsService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.clusterMgrApi = Objects.requireNonNull(clusterMgrApi);
        this.restTemplate = Objects.requireNonNull(restTemplate);
        this.apiWebsocketHandler = Objects.requireNonNull(apiWebsocketHandler);
        this.buildProperties = buildProperties;
        this.deploymentMode = deploymentMode;
        this.enableReporting = enableReporting;
        this.settingsService = settingsService;
    }

    @Override
    public LoggingApiDTO getLoggingLevels() {
        final Map<String, LoggingLevel> componentLoggingLevels = new HashMap<>();
        final Set<String> components = clusterService.getKnownComponents();
        for (String component : components) {
            if (component.equals(apiHost)) {
                // no need for rest call for API component since we are in API component
                final LoggerContext logContext = (LoggerContext)LogManager.getContext(false);
                componentLoggingLevels.put(apiHost, LoggingMapper.protoLogLevelToApiLogLevel(
                    LoggingUtils.log4jLevelToProtoLogLevel(
                            logContext.getLogger(LogConfigurationService.TURBO_PACKAGE_NAME)
                            .getLevel())));
            } else {
                getLoggingLevel(component).ifPresent(loggingLevel ->
                    componentLoggingLevels.put(component, loggingLevel));
            }
        }
        final LoggingApiDTO loggingApiDTO = new LoggingApiDTO();
        loggingApiDTO.setComponentLoggingLevel(componentLoggingLevels);
        return loggingApiDTO;
    }

    @Override
    public LoggingApiDTO setLoggingLevelForGivenComponent(final LoggingApiDTO loggingDTO)
            throws OperationFailedException {
        final Map<String, LoggingLevel> inputLoggingLevels = loggingDTO.getComponentLoggingLevel();
        if (inputLoggingLevels == null) {
            throw new IllegalArgumentException("LoggingApiDTO must not be null");
        }

        final Map<String, LoggingLevel> newLoggingLevelsByComponent = new HashMap<>();
        for (Map.Entry<String, LoggingLevel> entry : inputLoggingLevels.entrySet()) {
            final String component = entry.getKey();
            final LoggingLevel newLoggingLevel = entry.getValue();
            if (component.equals(apiHost)) {
                logger.info("Setting {} logging level to: {}",
                        LogConfigurationService.TURBO_PACKAGE_NAME, newLoggingLevel);
                // no need for rest call for API component since we are in API component
                Configurator.setLevel(LogConfigurationService.TURBO_PACKAGE_NAME,
                        LoggingUtils.protoLogLevelToLog4jLevel(
                                LoggingMapper.apiLogLevelToProtoLogLevel(newLoggingLevel)));
                newLoggingLevelsByComponent.put(component, newLoggingLevel);
            } else {
                setLoggingLevel(component, newLoggingLevel).ifPresent(level ->
                    newLoggingLevelsByComponent.put(component, level));
            }
        }
        final LoggingApiDTO loggingApiDTO = new LoggingApiDTO();
        loggingApiDTO.setComponentLoggingLevel(newLoggingLevelsByComponent);
        return loggingApiDTO;
    }

    /**
     * Get logging level for the given component. We do REST call here rather than gRPC call
     * because lots of components don't support gRPC server, for example: all probes, which don't
     * extend BaseVmtComponent; market, which doesn't override method buildGrpcServer, etc.
     *
     * @param component name of the component
     * @return optional logging level in the form of {@link LoggingLevel}, or empty if any error
     */
    private Optional<LoggingLevel> getLoggingLevel(@Nonnull String component) {
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(component)
            .port(httpPort)
            .path("/LogConfigurationService/getLogLevels");
        try {
            LogConfigurationServiceResponse logConfigResponse = restTemplate.postForObject(
                builder.build().toUriString(), new HttpEntity<>(GetLogLevelsRequest.getDefaultInstance()),
                LogConfigurationServiceResponse.class);
            if (logConfigResponse.error != null) {
                logger.error("Error getting logging levels for component {}: {}", component,
                    logConfigResponse.error);
                return Optional.empty();
            }

            final GetLogLevelsResponse logLevelsResponse = GSON.fromJson(
                GSON.toJson(logConfigResponse.response), GetLogLevelsResponse.class);
            final LogLevel turboLogLevel = logLevelsResponse.getLogLevelsMap().get(
                LogConfigurationService.TURBO_PACKAGE_NAME);
            if (turboLogLevel == null) {
                logger.error(
                        "Logging level for {} is not defined for component {}",
                        LogConfigurationService.TURBO_PACKAGE_NAME, component);
                return Optional.empty();
            }
            return Optional.of(LoggingMapper.protoLogLevelToApiLogLevel(turboLogLevel));
        } catch (ResourceAccessException e) {
            // component may be down; warn but don't flood the log with stack traces
            logger.warn("Unable to get logging levels for component {}", component);
            return Optional.empty();
        } catch (UnsupportedOperationException e) {
            logger.error("Proto logging level for component {} is not supported by API", component, e);
            return Optional.empty();
        }
    }

    /**
     * Set logging level for the given component. We do REST call here rather than gRPC call
     * because lots of components don't support gRPC server, for example: all probes, which don't
     * extend BaseVmtComponent; market, which doesn't override method buildGrpcServer, etc.
     *
     * @param component name of the component
     * @param newloggingLevel new logging level to set
     * @return optional logging level in the form of {@link LoggingLevel}, or empty if any error
     * @throws OperationFailedException if log levels could not be set
     */
    private Optional<LoggingLevel> setLoggingLevel(@Nonnull String component,
                                                   @Nonnull LoggingLevel newloggingLevel)
            throws OperationFailedException {
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(component)
            .port(httpPort)
            .path("/LogConfigurationService/setLogLevels");
        try {
            final SetLogLevelsRequest setLogLevelsRequest = SetLogLevelsRequest.newBuilder()
                .putLogLevels(LogConfigurationService.TURBO_PACKAGE_NAME,
                    LoggingMapper.apiLogLevelToProtoLogLevel(newloggingLevel))
                .build();
            LogConfigurationServiceResponse logConfigResponse = restTemplate.postForObject(
                builder.build().toUriString(), new HttpEntity<>(setLogLevelsRequest),
                LogConfigurationServiceResponse.class);
            if (logConfigResponse.error != null) {
                logger.error("Error setting logging levels for component {}: {}", component,
                    logConfigResponse.error);
                return Optional.empty();
            }
            return Optional.of(newloggingLevel);
        } catch (ResourceAccessException e) {
            // component may be down
            throw new OperationFailedException("Unable to set logging levels for component: " +
                component + ", error: " + e.getMessage());
        }
    }

    @Override
    public boolean exportDiagData() {
        invokeSchedulerToExportDiags();
        return true;
    }

    @Nonnull
    private static HttpProxyConfig convert(HttpProxyDTO src) {
        return new HttpProxyConfig(src.getIsProxyEnabled(), src.getProxyHost(),
                src.getProxyPortNumber(), src.getUserName(), src.getPassword());
    }

    @VisibleForTesting
    Future<Boolean> invokeSchedulerToExportDiags() {
        return scheduler.schedule(
            () -> {
                boolean exportedSucceed;
                try {
                    exportedSucceed = clusterMgrApi.exportComponentDiagnostics(
                        convert(retrieveProxyInfoFrom()));
                } catch (HttpClientErrorException e) {
                    logger.error("Failed to export diagnostics files with exception: ", e);
                    apiWebsocketHandler.broadcastDiagsExportNotification(
                        ExportNotification.newBuilder()
                            .setStatusNotification(ExportStatusNotification.newBuilder()
                                .setStatus(ExportStatus.FAILED)
                                .setDescription(e.getLocalizedMessage())
                                .build())
                            .build());
                    return false;
                } catch (RuntimeException e) {
                    logger.error("Failed to export diagnostics files with exception: ", e);
                    apiWebsocketHandler.broadcastDiagsExportNotification(
                        FAILED_TO_EXPORT_DIAGNOSTICS_FAILED);
                    return false;
                }
                if (exportedSucceed) {
                    logger.debug("Successfully exported diagnostics files");
                    apiWebsocketHandler.broadcastDiagsExportNotification(
                        EXPORTED_DIAGNOSTICS_SUCCEED);
                    return true;
                } else {
                    logger.error("Failed to export diagnostics files");
                    apiWebsocketHandler.broadcastDiagsExportNotification(
                        FAILED_TO_EXPORT_DIAGNOSTICS_FAILED);
                    return false;
                }
            }, 1, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean loadConfigFiles(final ConfigurationType configType, final String topology) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ProductVersionDTO getVersionInfo(boolean checkForUpdates) {
        final ProductVersionDTO product = makeProductVersion(buildProperties);
        product.setVersionInfo(getVersionInfoString());
        // TODO: 'checkForUpdates' is not yet implemented
        product.setUpdates(UPDATES_NOT_IMPLEMENTED);
        product.setMarketVersion(MARKET_VERSION);
        return product;
    }

    @Override
    public boolean updateAppliance() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public HttpProxyDTO getProxyInfo() {
        final HttpProxyDTO httpProxyDTO = retrieveProxyInfoFrom();
        if (StringUtils.isNotBlank(httpProxyDTO.getPassword())) {
            httpProxyDTO.setPassword(ClusterService.ASTERISKS);
        }
        return httpProxyDTO;
    }

    private HttpProxyDTO retrieveProxyInfoFrom() {
        final HttpProxyDTO httpProxyDTO = new HttpProxyDTO();
        // the password is encrypted, so it's secure at rest.
        // TODO enable secure at transition
        final String plainTextPassword = keyValueStore.get(PROXY_USER_PASSWORD)
            .map(CryptoFacility::decrypt).orElse("");
        httpProxyDTO.setIsProxyEnabled(Boolean.parseBoolean(keyValueStore.get(PROXY_ENABLED)
            .orElse("false")));
        httpProxyDTO.setUserName(keyValueStore.get(PROXY_USER_NAME).orElse(""));
        httpProxyDTO.setPassword(plainTextPassword);
        httpProxyDTO.setProxyHost(keyValueStore.get(PROXY_HOST).orElse(""));
        httpProxyDTO.setProxyPortNumber(keyValueStore.get(PROXY_PORT_NUMBER).map(Strings::parseInteger).orElse(null));
        return httpProxyDTO;
    }

    /**
     * TODO validate the correctness of user inputs. E.g:
     * Hostname: it should be either hostname or IP (only).
     * Port number: it should be valid port number (within the range).
     * Overall: validate if the proxy is actually available and usable.
     */
    @Override
    public HttpProxyDTO setProxyConfig(final HttpProxyDTO httpProxyDTO) throws InvalidOperationException {
        final boolean isProxyEnabled = httpProxyDTO.getIsProxyEnabled();
        // disable case, clean up all the configuration.
        if (!isProxyEnabled) {
            cleanUpProxyConfig();
            return EMPTY_PROXY_DTO;
        } else {
            final boolean isSecureProxy = StringUtils.isNotBlank(httpProxyDTO.getUserName());
            validateProxyInputs(httpProxyDTO, isProxyEnabled, isSecureProxy);
            storeProxyConfig(httpProxyDTO, isProxyEnabled, isSecureProxy);
            return httpProxyDTO;
        }
    }

    // Clean up proxy configuration.
    private void cleanUpProxyConfig() {
        keyValueStore.removeKey(PROXY_ENABLED);
        keyValueStore.removeKey(PROXY_HOST);
        keyValueStore.removeKey(PROXY_PORT_NUMBER);
        keyValueStore.removeKey(PROXY_USER_NAME);
        keyValueStore.removeKey(PROXY_USER_PASSWORD);
    }

    @VisibleForTesting
    void storeProxyConfig(@Nonnull final HttpProxyDTO httpProxyDTO,
                                 final boolean isProxyEnabled,
                                 final boolean isSecureProxy) {
        keyValueStore.put(PROXY_ENABLED, String.valueOf(isProxyEnabled));
        if (isSecureProxy) {
            keyValueStore.put(PROXY_USER_NAME, httpProxyDTO.getUserName());
            keyValueStore.put(PROXY_USER_PASSWORD, encrypt(httpProxyDTO.getPassword()));
        }
        keyValueStore.put(PROXY_HOST, httpProxyDTO.getProxyHost());
        keyValueStore.put(PROXY_PORT_NUMBER, Strings.toString(httpProxyDTO.getProxyPortNumber()));
        httpProxyDTO.setPassword(ClusterService.ASTERISKS);
    }

    private String encrypt(@Nonnull final String plainTextPassword) {
        if (ClusterService.ASTERISKS.equalsIgnoreCase(plainTextPassword)) {
            return ClusterService.ASTERISKS;
        }
        return CryptoFacility.encrypt(plainTextPassword);
    }

    private void validateProxyInputs(@Nonnull final HttpProxyDTO httpProxyDTO,
                                     final boolean isProxyEnabled,
                                     final boolean isSecureProxy) throws InvalidOperationException {
        final String plainTextPassword = httpProxyDTO.getPassword();
        // if user name is entered, the proxy is secured.
        // asterisks are masked password, and cannot be password itself.
        if (isSecureProxy && (StringUtils.isBlank(plainTextPassword) ||
            plainTextPassword.equalsIgnoreCase(ClusterService.ASTERISKS))) {
            final String message = "plainTextPassword cannot be asterisks or empty";
            logger.warn(message);
            throw new IllegalArgumentException(message);
        }
        if (isProxyEnabled) {
            if (StringUtils.isBlank(httpProxyDTO.getProxyHost()) || httpProxyDTO.getProxyPortNumber() == null) {
                final String message = "proxy server requires both host and port number";
                logger.warn(message);
                throw new InvalidOperationException(message);
            }
        }
    }

    @Override
    public SystemStatusApiDTO getSystemStatus() {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get the product capabilities.
     * <p>Return configurations control hiding or showing
     * certain areas of the UI</p>
     *
     * @return the {@link ProductCapabilityDTO}.
     */
    @Override
    public ProductCapabilityDTO getProductCapabilities() throws Exception {
        ProductCapabilityDTO productCapabilityDTO = new ProductCapabilityDTO();
        productCapabilityDTO.setDeploymentMode(this.deploymentMode);
        productCapabilityDTO.setReportingEnabled(this.enableReporting);
        return productCapabilityDTO;
    }

    /**
     * Get the status of Telemetry.
     * <p>Returns the status of whether telemetry should be enabled,
     * and whether the user should be shown an opt-in pop-up</p>
     *
     * @return the {@link TelemetryDTO}.
     */
    @Override
    public TelemetryDTO getTelemetryStatus() throws Exception {
        final SettingApiDTO<String> termsAcceptedSetting =
                this.settingsService.getSettingByUuidAndName("telemetrymanager",
                        TelemetryTermsAccepted.getSettingName());
        final SettingApiDTO<String> enabledSetting =
                this.settingsService.getSettingByUuidAndName("telemetrymanager",
                        TelemetryEnabled.getSettingName());
        return new TelemetryDTO().setTelemetryTermsViewed(Boolean.parseBoolean(termsAcceptedSetting.getValue()))
                .setTelemetryEnabled(Boolean.parseBoolean(enabledSetting.getValue()));
    }

    /**
     * Updates the settings for Telemetry.
     *
     * @param telemetryDTO From the api endpoint, a dto containing the settings to update
     * @return the {@link TelemetryDTO} with updated settings.
     */
    @Override
    public TelemetryDTO setTelemetryStatus(TelemetryDTO telemetryDTO) throws Exception {
        SettingApiDTO<String> termsAcceptedSetting = new SettingApiDTO<>();
        SettingApiDTO<String> enabledSetting = new SettingApiDTO<>();
        termsAcceptedSetting.setUuid(TelemetryTermsAccepted.getSettingName());
        termsAcceptedSetting.setValue(Boolean.toString(telemetryDTO.getTelemetryTermsViewed()));
        enabledSetting.setUuid(TelemetryEnabled.getSettingName());
        enabledSetting.setValue(Boolean.toString(telemetryDTO.getTelemetryEnabled()));
        enabledSetting = this.settingsService.putSettingByUuidAndName(
                "telemetrymanager", TelemetryEnabled.getSettingName(), enabledSetting
        );
        termsAcceptedSetting = this.settingsService.putSettingByUuidAndName(
                "telemetrymanager", TelemetryTermsAccepted.getSettingName(), termsAcceptedSetting
        );
        return new TelemetryDTO()
                .setTelemetryTermsViewed(Boolean.parseBoolean(termsAcceptedSetting.getValue()))
                .setTelemetryEnabled(Boolean.parseBoolean(enabledSetting.getValue()));
    }

    private String getVersionInfoString() {
        String header = MessageFormat.format(VERSION_INFO_HEADER, publicVersionString, buildNumber, buildTime);
        ClusterConfigurationDTO clusterConfig = clusterService.getClusterConfiguration();
        return header + clusterConfig.getDefaultProperties().entrySet().stream()
            .map(componentDefaultsEntry -> String.format("%s: %s", componentDefaultsEntry.getKey(),
                componentDefaultsEntry.getValue().get(COMPONENT_VERSION_KEY)))
            .sorted()
            .collect(Collectors.joining("\n"));
    }

    /**
     * Create a {@link ProductVersionDTO} filled with the git commit properties.
     *
     * @param buildProperties contains a breakdown of the different properties of the build job
     *                        for this component
     * @return The {@link ProductVersionDTO}.
     */
    @Nonnull
    private ProductVersionDTO makeProductVersion(@Nonnull final BuildProperties buildProperties) {
        final ProductVersionDTO productVersion = new ProductVersionDTO();
        productVersion.setBranch(buildProperties.getBranch());
        productVersion.setVersion(buildProperties.getVersion());
        productVersion.setBuild(buildProperties.getBuildTime());
        productVersion.setCommit(buildProperties.getShortCommitId());
        productVersion.setGitDescription(buildProperties.getCommitId() + " " +
            (buildProperties.isDirty() ? "dirty" : ""));
        productVersion.setHasCodeChanges(buildProperties.isDirty());
        return productVersion;
    }


}
