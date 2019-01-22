package com.vmturbo.api.component.external.api.service;

import java.text.MessageFormat;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
import com.vmturbo.api.dto.admin.SystemStatusApiDTO;
import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.enums.ConfigurationType;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.serviceinterfaces.IAdminService;

public class AdminService implements IAdminService {

    private static final String VERSION_INFO_HEADER = "Turbonomic Operations Manager {0} (Build {1}) {2}\n\n";

    private static final String UPDATES_NOT_IMPLEMENTED = "<not implemented>";

    @Value("${publicVersionString}")
    private String publicVersionString;

    @Value("${build-number.build}")
    private String buildNumber;

    @Value("${build-number.time}")
    private String buildTime;

    @Value("${build-number.package}")
    private String buildPackage;

    private final ClusterService clusterService;

    AdminService(@Nonnull final ClusterService clusterService) {
        this.clusterService = clusterService;
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
        throw ApiUtils.notImplementedInXL();
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
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public HttpProxyDTO setProxyConfig(final HttpProxyDTO maintenanceDTO) {
        throw ApiUtils.notImplementedInXL();
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
