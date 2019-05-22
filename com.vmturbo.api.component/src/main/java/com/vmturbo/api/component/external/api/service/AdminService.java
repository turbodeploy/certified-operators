package com.vmturbo.api.component.external.api.service;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.admin.SystemStatusApiDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.enums.ConfigurationType;
import com.vmturbo.api.enums.LoggingLevel;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.IAdminService;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.Logging.GetLogLevelsResponse;
import com.vmturbo.common.protobuf.logging.Logging.LogLevel;
import com.vmturbo.common.protobuf.logging.Logging.SetLogLevelsRequest;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController.LogConfigurationServiceResponse;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.utils.LoggingUtils;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.kvstore.KeyValueStore;

public class AdminService implements IAdminService {

    private static final String VERSION_INFO_HEADER = "Turbonomic Operations Manager {0} (Build {1}) {2}\n\n";

    private static final String UPDATES_NOT_IMPLEMENTED = "<not implemented>";

    // Following strings match user inputs.
    // Is proxy enabled?
    @VisibleForTesting
    public static final String PROXY_ENABLED = "proxyEnabled";

    // Proxy server user name.
    @VisibleForTesting
    public static final String PROXY_USER_NAME = "proxyUserName";

    // Proxy server password.
    @VisibleForTesting
    public static final String PROXY_USER_PASSWORD = "proxyUserPassword";

    // Proxy server host name.
    @VisibleForTesting
    public static final String PROXY_HOST = "proxyHost";

    // Proxy server port number.
    @VisibleForTesting
    public static final String PROXY_PORT_NUMBER = "proxyPortNumber";

    private final KeyValueStore keyValueStore;

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

    AdminService(@Nonnull final ClusterService clusterService,
                 @Nonnull final KeyValueStore keyValueStore,
                 @Nonnull final ClusterMgrRestClient clusterMgrApi,
                 @Nonnull final RestTemplate restTemplate) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.clusterMgrApi = Objects.requireNonNull(clusterMgrApi);
        this.restTemplate = Objects.requireNonNull(restTemplate);
    }

    @Override
    public LoggingApiDTO getLoggingLevels() {
        final Map<String, LoggingLevel> componentLoggingLevels = new HashMap<>();
        final Set<String> components = clusterService.getKnownComponents();
        for (String component : components) {
            if (component.equals(apiHost)) {
                // no need for rest call for API component since we are in API component
                final LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
                componentLoggingLevels.put(apiHost, LoggingUtils.protoLogLevelToApiLogLevel(
                    LoggingUtils.log4jLevelToProtoLogLevel(logContext.getRootLogger().getLevel())));
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
                logger.info("Setting root logging level to: {}", newLoggingLevel);
                // no need for rest call for API component since we are in API component
                Configurator.setRootLevel(LoggingUtils.protoLogLevelToLog4jLevel(
                    LoggingUtils.apiLogLevelToProtoLogLevel(newLoggingLevel)));
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
            final LogLevel rootLogLevel = logLevelsResponse.getLogLevelsMap().get(
                LogManager.ROOT_LOGGER_NAME);
            if (rootLogLevel == null) {
                logger.error("Root logging level is not defined for component {}", component);
                return Optional.empty();
            }
            return Optional.of(LoggingUtils.protoLogLevelToApiLogLevel(rootLogLevel));
        } catch (ResourceAccessException e) {
            // component may be down
            logger.error("Unable to get logging levels for component {}", component, e);
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
                .putLogLevels(LogManager.ROOT_LOGGER_NAME,
                    LoggingUtils.apiLogLevelToProtoLogLevel(newloggingLevel))
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
       return clusterMgrApi.exportComponentDiagnostics(retrieveProxyInfoFrom());
    }

    @Override
    public boolean loadConfigFiles(final ConfigurationType configType, final String topology) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ProductVersionDTO getVersionInfo(boolean checkForUpdates) {
        ProductVersionDTO answer = new ProductVersionDTO();
        answer.setVersionInfo(getVersionInfoString());
        // TODO: 'checkForUpdates' is not yet implemented
        answer.setUpdates(UPDATES_NOT_IMPLEMENTED);
        // xl uses market 2
        answer.setMarketVersion(2);
        return answer;
    }

    @Override
    public boolean updateAppliance() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public HttpProxyDTO getProxyInfo() {
        final HttpProxyDTO httpProxyDTO = retrieveProxyInfoFrom();
        if (!StringUtils.isEmpty(httpProxyDTO.getPassword())) {
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
        httpProxyDTO.setIsProxyEnabled(Boolean.valueOf(keyValueStore.get(PROXY_ENABLED).orElse("false")));
        httpProxyDTO.setUserName(keyValueStore.get(PROXY_USER_NAME).orElse(""));
        httpProxyDTO.setPassword(plainTextPassword);
        httpProxyDTO.setProxyHost(keyValueStore.get(PROXY_HOST).orElse(""));
        httpProxyDTO.setPortNumber(keyValueStore.get(PROXY_PORT_NUMBER).orElse(""));
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
        final boolean isSecureProxy = !StringUtils.isEmpty(httpProxyDTO.getUserName());
        validateProxyInputs(httpProxyDTO, isProxyEnabled, isSecureProxy);
        storeProxyConfig(httpProxyDTO, isProxyEnabled, isSecureProxy);
        return httpProxyDTO;
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
        keyValueStore.put(PROXY_PORT_NUMBER, httpProxyDTO.getPortNumber());
        httpProxyDTO.setPassword(ClusterService.ASTERISKS);
    }

    private String encrypt(@Nonnull final String plainTextPassword) {
        if (ClusterService.ASTERISKS.equalsIgnoreCase(plainTextPassword)) {
            return ClusterService.ASTERISKS;
        }
        return CryptoFacility.encrypt(plainTextPassword);
    }

    private void validateProxyInputs(@Nonnull final HttpProxyDTO httpProxyDTO,
                                     @Nonnull final boolean isProxyEnabled,
                                     final boolean isSecureProxy) throws InvalidOperationException {
        final String plainTextPassword = httpProxyDTO.getPassword();
        // if user name is entered, the proxy is secured.
        // asterisks are masked password, and cannot be password itself.
        if (isSecureProxy && (StringUtils.isEmpty(plainTextPassword) ||
            plainTextPassword.equalsIgnoreCase(ClusterService.ASTERISKS))) {
            final String message = "plainTextPassword cannot be asterisks or empty";
            logger.warn(message);
            throw new IllegalArgumentException(message);
        }
        if (isProxyEnabled) {
            if (StringUtils.isEmpty(httpProxyDTO.getProxyHost()) ||
                StringUtils.isEmpty(httpProxyDTO.getPortNumber())) {
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

    private String getVersionInfoString() {
        String header = MessageFormat.format(VERSION_INFO_HEADER, publicVersionString, buildNumber, buildTime);
        ClusterConfigurationDTO clusterConfig = clusterService.getClusterConfiguration();
        return header + clusterConfig.getInstances().values().stream()
                .filter(instanceInfo -> instanceInfo.getComponentVersion() != null)
                .map(instanceInfo -> String.format("%s: %s", instanceInfo.getComponentType(),
                        instanceInfo.getComponentVersion()))
                .collect(Collectors.joining("\n"));
    }
}
