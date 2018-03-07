package com.vmturbo.api.component.external.api.service;

import java.text.MessageFormat;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.admin.HttpProxyDTO;
import com.vmturbo.api.dto.admin.LoggingApiDTO;
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
    public LoggingApiDTO getLoggingLevels() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LoggingApiDTO setLoggingLevelForGivenComponent(final LoggingApiDTO loggingDTO)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean exportDiagData() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean loadConfigFiles(final ConfigurationType configType, final String topology)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ProductVersionDTO getVersionInfo() throws Exception {
        ProductVersionDTO answer = new ProductVersionDTO();
        answer.setVersionInfo(getVersionInfoString());
        answer.setUpdates(UPDATES_NOT_IMPLEMENTED);
        return answer;
    }

    @Override
    public boolean updateAppliance() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public HttpProxyDTO getProxyInfo() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public HttpProxyDTO setProxyConfig(final HttpProxyDTO maintenanceDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    private String getVersionInfoString() {
        String header = MessageFormat.format(VERSION_INFO_HEADER, publicVersionString, buildNumber, buildTime);
        ClusterConfigurationDTO clusterConfig = clusterService.getClusterConfiguration();
        String componentVersions = clusterConfig.getInstances().values().stream()
                .filter(instanceInfo -> instanceInfo.getComponentVersion() != null)
                .map(instanceInfo -> new StringBuilder(instanceInfo.getComponentType())
                        .append(": ")
                        .append(instanceInfo.getComponentVersion())
                        .toString())
                .collect(Collectors.joining("\n"));
        return new StringBuilder(header).append(componentVersions).toString();
    }
}
