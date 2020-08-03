package com.vmturbo.api.internal.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.service.AdminService;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricDefinitions;

/**
 * Appliance version diagnostics provider.
 */
public class VersionDiagnosable implements StringDiagnosable {

    private static final Logger logger = LogManager.getLogger();

    private static final String VERSION_FILE_NAME = "turbonomic_cluster_version.txt";

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final AdminService adminService;

    private final ClusterMgrRestClient clusterService;

    private final long liveTopologyContextId;

    /**
     * Constructs diagnostics provider.
     *
     * @param supplyChainFetcherFactory supply chain factory
     * @param adminService admin service
     * @param clusterManagerClient cluster manager client
     * @param liveTopologyContextId live topology context id
     */
    public VersionDiagnosable(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                 @Nonnull final AdminService adminService,
                                 @Nonnull ClusterMgrRestClient clusterManagerClient,
                                 final long liveTopologyContextId) {
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.adminService = Objects.requireNonNull(adminService);
        this.clusterService = Objects.requireNonNull(clusterManagerClient);
        this.liveTopologyContextId = liveTopologyContextId;
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        ProductVersionDTO versionDTO = new ProductVersionDTO();
        try {
            versionDTO = adminService.getVersionInfo(true);
        } catch (Exception e) {
            logger.error("Error fetching production version: ", e);
        }

        // Collect telemetry only if user has enabled telemetry.
        if (clusterService.isTelemetryEnabled()) {
            // Collect telemetry metrics to Prometheus before writing out the Prometheus metrics.
            collectTelemetryMetrics(versionDTO);
        }
        for (String string: versionInfo(versionDTO)) {
            appender.appendString(string);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return VERSION_FILE_NAME;
    }

    @VisibleForTesting
    @Nonnull
    List<String> versionInfo(@Nonnull final ProductVersionDTO versionDTO) {
        return Arrays.asList(versionDTO.getVersionInfo() + "\n",
            "Updates: " + versionDTO.getUpdates(),
            "Market version: " + versionDTO.getMarketVersion());
    }

    private void collectTelemetryMetrics(@Nonnull final ProductVersionDTO versionDTO) {
        final VersionAndRevision versionAndRevision = new VersionAndRevision(versionDTO.getVersionInfo());
        TelemetryMetricDefinitions.setTurbonomicVersionAndRevision(
            versionAndRevision.version, versionAndRevision.revision);

        // Disabling this call as it makes an rpc call to repository. If
        // repository is slow, api diag dump will be blocked for a while.
        //collectEntityCounts(versionAndRevision);
    }

    @VisibleForTesting
    void collectEntityCounts(@Nonnull final VersionAndRevision versionAndRevision) {
        try {
            final SupplychainApiDTOFetcherBuilder supplyChainFetcher = this.supplyChainFetcherFactory
                .newApiDtoFetcher()
                .topologyContextId(liveTopologyContextId)
                .addSeedUuids(Collections.emptyList())
                .entityDetailType(null)
                .includeHealthSummary(false);

            final SupplychainApiDTO supplyChain = supplyChainFetcher.fetch();
            supplyChain.getSeMap().forEach((entityType, entry) ->
                // TODO: Add support for understanding target type
                TelemetryMetricDefinitions.setServiceEntityCount(entityType, "", entry.getEntitiesCount(),
                    versionAndRevision.version, versionAndRevision.revision));
        } catch (Exception e) {
            logger.error("Unable to collect supply chain entity counts.", e);
        }
    }

    /**
     * Capture the build version and revision from a string that looks like:
     * {@code
     * Turbonomic Operations Manager 7.4.0 (Build \"20180722153849000\") \"2018-07-24 12:26:58\"
     * }
     */
    @VisibleForTesting
    @Immutable
    static class VersionAndRevision {
        public final String version;
        public final String revision;

        VersionAndRevision(@Nonnull final String versionInfo) {
            String parsedVersion = "";
            String parsedRevision = "";

            try {
                // Should be a string of the form:
                // Turbonomic Operations Manager 7.4.0 (Build \"20180722153849000\") \"2018-07-24 12:26:58\"
                final String turbonomicVersionLine = versionInfo.split("\n")[0];
                final String[] versionComponents = turbonomicVersionLine.split("\\s+");
                parsedVersion = Stream.of(0, 1, 2, 3)
                    .map(i -> versionComponents[i])
                    .collect(Collectors.joining(" "));
                final String revisionString = versionComponents[5];
                parsedRevision = revisionString.substring(1, revisionString.length() - 2);
            } catch (Exception e) {
                logger.error("Unable to parse version string " + versionInfo, e);
            }

            this.version = parsedVersion;
            this.revision = parsedRevision;
        }
    }
}
