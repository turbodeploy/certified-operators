package com.vmturbo.api.component.external.api.service;

import java.text.MessageFormat;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.admin.SystemStatusApiDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.enums.ConfigurationType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.serviceinterfaces.IAdminService;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
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

    private final ClusterService clusterService;

    private final Logger log = LogManager.getLogger();

    private ClusterMgrRestClient clusterMgrApi;

    AdminService(@Nonnull final ClusterService clusterService,
                 @Nonnull final KeyValueStore keyValueStore,
                 @Nonnull final ClusterMgrRestClient clusterMgrApi) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.clusterMgrApi = Objects.requireNonNull(clusterMgrApi);
    }

    @Override
    public LoggingApiDTO getLoggingLevels() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LoggingApiDTO setLoggingLevelForGivenComponent(final LoggingApiDTO loggingDTO) {
        throw ApiUtils.notImplementedInXL();
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
            log.warn(message);
            throw new IllegalArgumentException(message);
        }
        if (isProxyEnabled) {
            if (StringUtils.isEmpty(httpProxyDTO.getProxyHost()) ||
                StringUtils.isEmpty(httpProxyDTO.getPortNumber())) {
                final String message = "proxy server requires both host and port number";
                log.warn(message);
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
