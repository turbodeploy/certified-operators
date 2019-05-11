package com.vmturbo.api.internal.controller;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.api.component.external.api.service.AdminService;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricDefinitions;

public class ApiDiagnosticsHandler {

    private static final Logger logger = LogManager.getLogger();

    private final DiagnosticsWriter diagnosticsWriter;

    public static final String VERSION_FILE_NAME = "turbonomic_cluster_version.txt";

    private static final String ERRORS_FILE = "dump_errors";

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final AdminService adminService;

    private final ClusterMgrRestClient clusterService;

    private final long liveTopologyContextId;

    public ApiDiagnosticsHandler(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                 @Nonnull final AdminService adminService,
                                 @Nonnull ClusterMgrRestClient clusterManagerClient,
                                 @Nonnull final DiagnosticsWriter diagnosticsWriter,
                                 final long liveTopologyContextId) {
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
        this.adminService = Objects.requireNonNull(adminService);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
        this.clusterService = Objects.requireNonNull(clusterManagerClient);
        this.liveTopologyContextId = liveTopologyContextId;
    }

    /**
     * Dumps the API component state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     * @return The list of errors encountered, or an empty list if successful.
     */
    public List<String> dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final List<String> errors = new ArrayList<>();

        ProductVersionDTO versionDTO = new ProductVersionDTO();
        try {
            versionDTO = adminService.getVersionInfo(true);
        } catch (Exception e) {
            logger.error("Error fetching production version: ", e);
        }

        // Version information (capture regardless of whether telemetry is enabled or not)
        try {
            diagnosticsWriter.writeZipEntry(VERSION_FILE_NAME, versionInfo(versionDTO), diagnosticZip);
        } catch (Exception e) {
            logger.error("Error writing version information: ", e);
            errors.add(e.getMessage());
        }

        // Collect telemetry only if user has enabled telemetry.
        if (clusterService.isTelemetryEnabled()) {
            // Collect telemetry metrics to Prometheus before writing out the Prometheus metrics.
            collectTelemetryMetrics(versionDTO);
            diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry, diagnosticZip);
        }

        if (!errors.isEmpty()) {
            diagnosticsWriter.writeZipEntry(ERRORS_FILE, errors, diagnosticZip);
        }

        return errors;
    }

    public List<String> restore(@Nonnull final InputStream inputStream) {
        // Nothing to do
        return Collections.emptyList();
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

    @VisibleForTesting
    @Immutable
    static class VersionAndRevision {
        public final String version;
        public final String revision;

        public VersionAndRevision(@Nonnull final String versionInfo) {
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
