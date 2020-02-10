package com.vmturbo.repository.diagnostics;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * Class to dump and restore topology in/to Arango DB.
 */
public class TopologyDiagnostics implements BinaryDiagsRestorable {

    private static final Logger logger = LogManager.getLogger();

    private final ArangoDump arangoDump;

    private final ArangoRestore arangoRestore;
    private final TopologyLifecycleManager topologyLifecycleManager;

    private final RestTemplate restTemplate;
    private final TopologyType topologyType;
    private final String diagsFileName;

    private final String arangoDatabaseName;

    /**
     * Constructs topology diagnostics.
     *
     * @param arangoDump arango dump
     * @param arangoRestore arango restore
     * @param topologyLifecycleManager topology manager
     * @param restTemplate REST template
     * @param topologyType topology type
     * @param diagsFileName file name to export diags to
     */
    public TopologyDiagnostics(@Nonnull final ArangoDump arangoDump,
            @Nonnull final ArangoRestore arangoRestore,
            @Nonnull final TopologyLifecycleManager topologyLifecycleManager,
            @Nonnull final RestTemplate restTemplate, @Nonnull final TopologyType topologyType,
            @Nonnull final String diagsFileName, @Nonnull final String arangoDatabaseName) {
        this.arangoDump = Objects.requireNonNull(arangoDump);
        this.arangoRestore = Objects.requireNonNull(arangoRestore);
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.restTemplate = Objects.requireNonNull(restTemplate);
        this.topologyType = Objects.requireNonNull(topologyType);
        this.diagsFileName = Objects.requireNonNull(diagsFileName);
        this.arangoDatabaseName = arangoDatabaseName;
    }

    @Override
    public void restoreDiags(@Nonnull byte[] bytes) throws DiagnosticsException {
        if (bytes.length == 0) {
            logger.info("Database dump for " + arangoDatabaseName + " is empty. Skipping it...");
            return;
        }
        final List<String> errors = new ArrayList<>();

        final Optional<TopologyID> topologyID =
                topologyLifecycleManager.getRealtimeTopologyId(topologyType);
        if (!topologyID.isPresent()) {
            throw new DiagnosticsException(
                    "Could not locate live topology to restore by type " + topologyType);
        }
        /*
         * Restore topology
         * Since we restored the IDs earlier, and we had originally dumped this topology,
         * (which only works if the topology is in the lifecycle manager) the lifecycle
         * manager should have an entry for the realtime database.
         */
        final TopologyID tid = topologyID.get();
        final String database = arangoDatabaseName;
        final String fullRestoreUrl = arangoRestore.getEndpoint() + "/" + database;

        final MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        // This is required in order to get the Flask server in the arangodb container
        // to recognize the input as a file resource.
        map.add("file", new ByteArrayResource(bytes) {
            @Override
            public String getFilename() {
                return "random-file-name";
            }
        });

        final HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(map);
        logger.info("Restoring database with request to {}", fullRestoreUrl);
        try {
            final ResponseEntity<String> responseEntity =
                    restTemplate.postForEntity(fullRestoreUrl, request, String.class);

            if (responseEntity.getStatusCode() != HttpStatus.CREATED) {
                errors.add("Failed to restore " + arangoDatabaseName + " database: " +
                        responseEntity.getBody());
            } else {
                logger.info("Restored " + arangoDatabaseName + " database.");
            }
        } catch (RestClientException e) {
            errors.add("POST to arangodb to restore topology failed with exception: " +
                    e.getLocalizedMessage());
        }

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Override
    public void collectDiags(@Nonnull OutputStream outputStream)
            throws DiagnosticsException, IOException {
        // We don't save real-time topology data in ArangoDB anymore. When dumping the diags,
        // sourceTopologyId and projectedTopologyId are always not present here and
        // "source_topology_dump" and "projected_topology_dump" files won't be correctly exported.
        // Even if we invoke the subsequent method to dump ArangoDB, the connection to fullDumpUrl
        // (arangodb:8599/dump/<db_name>) will be failed.
        // TODO we need to either refactor this to make ArangoDB dump work or remove the corresponding
        // logic if we decide ArangoDB dump is not needed in the diags.
        final Optional<TopologyID> topologyID =
                topologyLifecycleManager.getRealtimeTopologyId(topologyType);
        if (!topologyID.isPresent()) {
            throw new DiagnosticsException(
                    topologyType + " topology is not present. Skipping dump");
        }
        final TopologyID tid = topologyID.get();
        final String db = arangoDatabaseName;

        logger.info("Dumping real-time topology with database {}, {}", db, tid);

        // Dump the specified topology from arangodb.
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(10000)
                        .setSocketTimeout(10000)
                        .build())
                .build()) {
            final String fullDumpUrl = arangoDump.getEndpoint() + "/" + db;
            logger.info("Dumping topology database by calling {}", fullDumpUrl);
            final HttpGet get = new HttpGet(fullDumpUrl);
            try (CloseableHttpResponse response = httpClient.execute(get)) {
                if (response.getStatusLine().getStatusCode() == org.apache.http.HttpStatus.SC_OK) {
                    IOUtils.copy(response.getEntity().getContent(), outputStream);
                    logger.info("Finished dumping database {}", db);
                } else {
                    final String errorResponse =
                            IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8);
                    throw new DiagnosticsException(
                            String.format("Fail to dump database %d HTTP status %d: %s", db,
                                    response.getStatusLine().getStatusCode(), errorResponse));
                }
            }
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return diagsFileName;
    }
}
