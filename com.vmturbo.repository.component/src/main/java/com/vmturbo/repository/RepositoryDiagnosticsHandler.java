package com.vmturbo.repository;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.repository.topology.TopologyDatabases;
import com.vmturbo.repository.topology.TopologyIDManager;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;

/**
 * The {@link RepositoryDiagnosticsHandler} controls the dumping and restoring of the Repository's
 * internal state.
 * <p>
 * The state has two parts:
 * 1) Various files collected from {@link Diagnosable} objects injected into the handler,
 *    such as the {@link TopologyRelationshipRecorder}.
 * 2) A binary file representing the Arangodb state collected via arangodump. See
 *    arango_dump_restore.py in com.vmturbo.arangodb for the python server that we run
 *    on the arangodb container to collect the state.
 * 3) (optional) An error file containing errors encountered when trying to dump state.
 */
public class RepositoryDiagnosticsHandler {

    private final Logger logger = LoggerFactory.getLogger(RepositoryDiagnosticsHandler.class);

    /**
     * The file name for the state of the {@link TopologyRelationshipRecorder}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String SUPPLY_CHAIN_RELATIONSHIP_FILE = "provider-rels.diags";

    /**
     * The file name for the state of the {@link TopologyIDManager}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String ID_MGR_FILE = "database-metadata.diags";

    /**
     * The file name for the topology dump collected from the arangodb container.
     * It's a binary file, so the "binary" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String TOPOLOGY_DUMP_FILE = "topology_dump.binary";

    @VisibleForTesting
    static final String ERRORS_FILE = "dump_errors";

    private final ArangoDump arangoDump;

    private final ArangoRestore arangoRestore;

    private final TopologyRelationshipRecorder globalSupplyChainRecorder;

    private final TopologyIDManager topologyIDManager;

    private final RestTemplate restTemplate;

    private final RecursiveZipReaderFactory zipReaderFactory;

    private final DiagnosticsWriter diagnosticsWriter;

    public RepositoryDiagnosticsHandler(final ArangoDump arangoDump,
                                        final ArangoRestore arangoRestore,
                                        final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                        final TopologyIDManager topologyIDManager,
                                        final RestTemplate restTemplate,
                                        final RecursiveZipReaderFactory zipReaderFactory,
                                        final DiagnosticsWriter diagnosticsWriter) {
        this.arangoDump = Objects.requireNonNull(arangoDump);
        this.arangoRestore = Objects.requireNonNull(arangoRestore);
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);
        this.topologyIDManager = Objects.requireNonNull(topologyIDManager);
        this.restTemplate = Objects.requireNonNull(restTemplate);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
    }

    /**
     * Dumps the repository state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     * @return The list of errors encountered, or an empty list if successful.
     */
    public List<String> dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final Optional<TopologyID> topologyId = topologyIDManager.getCurrentRealTimeTopologyId();

        final List<String> errors = new ArrayList<>();
        if (!topologyId.isPresent()) {
            errors.add("No real-time topology found.");
        } else {
            final TopologyID tid = topologyId.get();
            final String db = topologyIDManager.databaseName(tid);

            logger.info("Dumping real-time topology with database {}, {}", db, tid);

            // Dump the specified topology from arangodb.
            final String fullDumpUrl = arangoDump.getEndpoint() + "/" + db;
            logger.info("Dumping topology database by calling {}", fullDumpUrl);
            try {
                final ResponseEntity<byte[]> databaseDumpEntity =
                        restTemplate.getForEntity(fullDumpUrl, byte[].class);
                if (databaseDumpEntity.getStatusCode() == HttpStatus.OK) {
                    diagnosticsWriter.writeZipEntry(TOPOLOGY_DUMP_FILE,
                            databaseDumpEntity.getBody(), diagnosticZip);
                    logger.info("Finished dumping topology");
                } else {
                    errors.add("Fail to dump topology database: "
                            + new String(databaseDumpEntity.getBody()));
                }
            } catch (RestClientException e) {
                errors.add("Error retrieving ArangoDB dump from the remote service: " +
                        e.getLocalizedMessage());
            }

            // Dumps the SE provider relationship
            logger.info("Dumping provider relationships");
            diagnosticsWriter.writeZipEntry(SUPPLY_CHAIN_RELATIONSHIP_FILE,
                    globalSupplyChainRecorder.collectDiags(), diagnosticZip);

            // Dumps the topology id and database name
            logger.info("Dumping topology IDs and database names");
            diagnosticsWriter.writeZipEntry(ID_MGR_FILE,
                    topologyIDManager.collectDiags(), diagnosticZip);
        }

        if (!errors.isEmpty()) {
            diagnosticsWriter.writeZipEntry(ERRORS_FILE, errors, diagnosticZip);
        }
        return errors;
    }

    /**
     * Restores the internal state as dumped by
     * {@link RepositoryDiagnosticsHandler#dump(ZipOutputStream)}.
     *
     * @param zis The zip input stream, as written to by
     *            {@link RepositoryDiagnosticsHandler#dump(ZipOutputStream)}.
     */
    @Nonnull
    public List<String> restore(@Nonnull final InputStream zis) {
        final List<String> errors = new ArrayList<>();
        // We can only successfully restore the topology dump to Arango if
        // we restore the topology ID to the internal state.
        // In order to be able to handle arbitrary orders of input files,
        // defer the topology dump restoration.
        boolean idRestored = false;
        Optional<Diags> topoDumpDiags = Optional.empty();

        for (Diags diags : zipReaderFactory.createReader(zis)) {
            final String name = diags.getName();
            if (name.equals(ID_MGR_FILE)) {
                if (diags.getLines() == null) {
                    errors.add("The file " + ID_MGR_FILE + " was not saved as lines of strings " +
                            "with the appropriate suffix!");
                } else {
                    topologyIDManager.restoreDiags(diags.getLines());
                    idRestored = true;
                    logger.info("Restored {} ", ID_MGR_FILE);
                }
            } else if (name.equals(SUPPLY_CHAIN_RELATIONSHIP_FILE)) {
                if (diags.getLines() == null) {
                    errors.add("The file " + SUPPLY_CHAIN_RELATIONSHIP_FILE + " was not saved as" +
                            " lines of strings with the appropriate suffix!");
                } else {
                    globalSupplyChainRecorder.restoreDiags(diags.getLines());
                    logger.info("Restored {} ", SUPPLY_CHAIN_RELATIONSHIP_FILE);
                }
            } else if (name.equals(TOPOLOGY_DUMP_FILE)) {
                // We'll handle this later, if the Repository's internal state gets initialized
                // correctly.
                topoDumpDiags = Optional.of(diags);
            } else {
                logger.warn("Skipping file: {}", name);
            }
        }

        // Restore the topology in ArangoDB.
        if (idRestored) {
            if (!topoDumpDiags.isPresent()) {
                errors.add("Did not find the file " + TOPOLOGY_DUMP_FILE +
                        " in the uploaded diags.");
            }
            topoDumpDiags.ifPresent(topoDump -> {
                if (topoDump.getBytes() == null) {
                    errors.add("The file " + TOPOLOGY_DUMP_FILE + " was not saved as" +
                            " a binary file with the appropriate suffix!");
                } else {
                    // Restore topology
                    final String database = TopologyDatabases.getDbName(
                            topologyIDManager.currentRealTimeDatabase().get());
                    final String fullRestoreUrl = arangoRestore.getEndpoint() + "/" + database;

                    final MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
                    // This is required in order to get the Flask server in the arangodb container
                    // to recognize the input as a file resource.
                    map.add("file", new ByteArrayResource(topoDump.getBytes()) {
                        @Override
                        public String getFilename() {
                            return TOPOLOGY_DUMP_FILE;
                        }
                    });

                    final HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(map);
                    logger.info("Restoring topology with request to {}", fullRestoreUrl);
                    try {
                        final ResponseEntity<String> responseEntity =
                                restTemplate.postForEntity(fullRestoreUrl, request, String.class);

                        if (responseEntity.getStatusCode() != HttpStatus.CREATED) {
                            errors.add("Failed to restore topology: " + responseEntity.getBody());
                        } else {
                            logger.info("Restored topology from {}", TOPOLOGY_DUMP_FILE);
                        }
                    } catch (RestClientException e) {
                        errors.add("POST to arangodb to restore topology failed with exception: " +
                                e.getLocalizedMessage());
                    }
                }
            });
        } else {
            errors.add("Did not successfully restore the realtime topology ID - was " +
                    ID_MGR_FILE + " not in the diags?");
        }
        return errors;
    }
}
