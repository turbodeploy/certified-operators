package com.vmturbo.repository;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

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

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
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

    private final Logger logger = LogManager.getLogger();

    /**
     * The file name for the state of the {@link TopologyRelationshipRecorder}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String SUPPLY_CHAIN_RELATIONSHIP_FILE = "provider-rels.diags";

    /**
     * The file name for the state of the {@link TopologyLifecycleManager}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String ID_MGR_FILE = "database-metadata.diags";

    /**
     * The file name for the source topology dump collected from the arangodb container.
     * It's a binary file, so the "binary" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String SOURCE_TOPOLOGY_DUMP_FILE = "source_topology_dump.binary";

    /**
     * The file name for the projected topology dump collected from the arangodb container.
     * It's a binary file, so the "binary" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String PROJECTED_TOPOLOGY_DUMP_FILE = "projected_topology_dump.binary";

    @VisibleForTesting
    static final String ERRORS_FILE = "dump_errors";

    private final TopologyRelationshipRecorder globalSupplyChainRecorder;

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyDiagnostics topologyDiagnostics;

    private final RecursiveZipReaderFactory zipReaderFactory;

    private final DiagnosticsWriter diagnosticsWriter;

    public RepositoryDiagnosticsHandler(final ArangoDump arangoDump,
                                        final ArangoRestore arangoRestore,
                                        final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                        final TopologyLifecycleManager topologyLifecycleManager,
                                        final RestTemplate restTemplate,
                                        final RecursiveZipReaderFactory zipReaderFactory,
                                        final DiagnosticsWriter diagnosticsWriter) {
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
        this.topologyDiagnostics = new DefaultTopologyDiagnostics(arangoDump, arangoRestore,
                topologyLifecycleManager, restTemplate);
    }

    @VisibleForTesting
    RepositoryDiagnosticsHandler(final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                        final TopologyLifecycleManager topologyLifecycleManager,
                                        final RecursiveZipReaderFactory zipReaderFactory,
                                        final DiagnosticsWriter diagnosticsWriter,
                                        final TopologyDiagnostics topologyDiagnostics) {
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
        this.topologyDiagnostics = Objects.requireNonNull(topologyDiagnostics);
    }

    /**
     * Dumps the repository state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     * @return The list of errors encountered, or an empty list if successful.
     */
    public List<String> dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final List<String> errors = new ArrayList<>();

        final Optional<TopologyID> sourceTopologyId =
                topologyLifecycleManager.getRealtimeTopologyId(TopologyType.SOURCE);
        if (!sourceTopologyId.isPresent()) {
            errors.add("No source real-time topology found.");
        } else {
            try {
                final byte[] srcTopology = topologyDiagnostics.dumpTopology(sourceTopologyId.get());
                diagnosticsWriter.writeZipEntry(SOURCE_TOPOLOGY_DUMP_FILE, srcTopology, diagnosticZip);
            } catch (DiagnosticsException e) {
                errors.addAll(e.getErrors());
            }
        }

        final Optional<TopologyID> projectedTopologyId =
                topologyLifecycleManager.getRealtimeTopologyId(TopologyType.PROJECTED);
        if (!projectedTopologyId.isPresent()) {
            errors.add("No projected real-time topology found.");
        } else {
            try {
                final byte[] projectedTopology = topologyDiagnostics.dumpTopology(projectedTopologyId.get());
                diagnosticsWriter.writeZipEntry(PROJECTED_TOPOLOGY_DUMP_FILE, projectedTopology, diagnosticZip);
            } catch (DiagnosticsException e) {
                errors.addAll(e.getErrors());
            }
        }

        // Dumps the SE provider relationship
        logger.info("Dumping provider relationships");
        try {
            diagnosticsWriter.writeZipEntry(SUPPLY_CHAIN_RELATIONSHIP_FILE,
                globalSupplyChainRecorder.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
        }

        // Dumps the topology id and database name
        logger.info("Dumping topology IDs and database names");
        try {
            diagnosticsWriter.writeZipEntry(ID_MGR_FILE,
                topologyLifecycleManager.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
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
        Optional<Diags> sourceTopoDumpDiags = Optional.empty();
        Optional<Diags> projectedTopoDumpDiags = Optional.empty();

        for (Diags diags : zipReaderFactory.createReader(zis)) {
            final String name = diags.getName();
            if (name.equals(ID_MGR_FILE)) {
                if (diags.getLines() == null) {
                    errors.add("The file " + ID_MGR_FILE + " was not saved as lines of strings " +
                            "with the appropriate suffix!");
                } else {
                    try {
                        topologyLifecycleManager.restoreDiags(diags.getLines());
                        idRestored = true;
                        logger.info("Restored {} ", ID_MGR_FILE);
                    } catch (DiagnosticsException e) {
                        errors.addAll(e.getErrors());
                    }
                }
            } else if (name.equals(SUPPLY_CHAIN_RELATIONSHIP_FILE)) {
                if (diags.getLines() == null) {
                    errors.add("The file " + SUPPLY_CHAIN_RELATIONSHIP_FILE + " was not saved as" +
                            " lines of strings with the appropriate suffix!");
                } else {
                    try {
                        globalSupplyChainRecorder.restoreDiags(diags.getLines());
                        logger.info("Restored {} ", SUPPLY_CHAIN_RELATIONSHIP_FILE);
                    } catch (DiagnosticsException e) {
                        errors.addAll(e.getErrors());
                    }
                }
            } else if (name.equals(SOURCE_TOPOLOGY_DUMP_FILE)) {
                // We'll handle this later, if the Repository's internal state gets initialized
                // correctly.
                sourceTopoDumpDiags = Optional.of(diags);
            } else if (name.equals(PROJECTED_TOPOLOGY_DUMP_FILE)) {
                // We'll handle this later, if the Repository's internal state gets initialized
                // correctly.
                projectedTopoDumpDiags = Optional.of(diags);
            } else {
                logger.warn("Skipping file: {}", name);
            }
        }

        // Restore the topology in ArangoDB.
        if (idRestored) {
            try {
                topologyDiagnostics.restoreTopology(sourceTopoDumpDiags, TopologyType.SOURCE);
            } catch (DiagnosticsException e) {
                errors.addAll(e.getErrors());
            }

            try {
                topologyDiagnostics.restoreTopology(projectedTopoDumpDiags, TopologyType.PROJECTED);
            } catch (DiagnosticsException e) {
                errors.addAll(e.getErrors());
            }
        } else {
            errors.add("Did not successfully restore the realtime topology ID - was " +
                    ID_MGR_FILE + " not in the diags?");
        }
        return errors;
    }

    /**
     * An interface to abstract away the details of interacting with the arango_dump_restore.py
     * web server in ArangoDB.
     * <p>
     * Mostly here for testing purposes (and also because it's cleaner :)).
     */
    @VisibleForTesting
    interface TopologyDiagnostics {

        /**
         * Restore a topology dumped by {@link TopologyDiagnostics#dumpTopology(TopologyID)}.
         *
         * @param diags An optional containing the {@link Diags} object.
         *              The {@link Diags#getBytes()} method should return the bytes
         *              returned by {@link TopologyDiagnostics#dumpTopology(TopologyID)}.
         * @param topologyType The type of the topology.
         * @throws DiagnosticsException
         */
        void restoreTopology(@Nonnull final Optional<Diags> diags,
                             @Nonnull final TopologyType topologyType) throws DiagnosticsException;

        /**
         * Dump the topology identified by the {@link TopologyID}.
         *
         * @param tid The {@link TopologyID} to dump.
         * @return The byte array containing the dumped topology.
         * @throws DiagnosticsException If there is an issue collecting diagnostics.
         */
        @Nonnull
        byte[] dumpTopology(@Nonnull TopologyID tid) throws DiagnosticsException;
    }

    @VisibleForTesting
    static class DefaultTopologyDiagnostics implements TopologyDiagnostics {
        private static final Logger logger = LogManager.getLogger();

        private final ArangoDump arangoDump;

        private final ArangoRestore arangoRestore;
        private final TopologyLifecycleManager topologyLifecycleManager;

        private final RestTemplate restTemplate;

        @VisibleForTesting
        DefaultTopologyDiagnostics(@Nonnull final ArangoDump arangoDump,
                                           @Nonnull final ArangoRestore arangoRestore,
                                           @Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                           @Nonnull final RestTemplate restTemplate) {
            this.arangoDump = Objects.requireNonNull(arangoDump);
            this.arangoRestore = Objects.requireNonNull(arangoRestore);
            this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
            this.restTemplate = Objects.requireNonNull(restTemplate);
        }

        @Nonnull
        @Override
        public void restoreTopology(@Nonnull final Optional<Diags> diags,
                                    @Nonnull final TopologyType topologyType)
                throws DiagnosticsException {
            final List<String> errors = new LinkedList<>();
            if (!diags.isPresent()) {
                errors.add("Did not find the " + topologyType + " topology in the uploaded diags.");
            }

            diags.ifPresent(topoDump -> {
                if (topoDump.getBytes() == null) {
                    errors.add("The file for the " + topologyType + " topology was not saved as" +
                            " a binary file with the appropriate suffix!");
                } else {
                    // Restore topology
                    // Since we restored the IDs earlier, and we had originally dumped this topology,
                    // (which only works if the topology is in the lifecycle manager) the lifecycle
                    // manager should have an entry for the realtime database.
                    final TopologyID tid =
                            topologyLifecycleManager.getRealtimeTopologyId(topologyType).get();
                    final String database = tid.toDatabaseName();
                    final String fullRestoreUrl = arangoRestore.getEndpoint() + "/" + database;

                    final MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
                    // This is required in order to get the Flask server in the arangodb container
                    // to recognize the input as a file resource.
                    map.add("file", new ByteArrayResource(topoDump.getBytes()) {
                        @Override
                        public String getFilename() {
                            return "random-file-name";
                        }
                    });

                    final HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(map);
                    logger.info("Restoring topology with request to {}", fullRestoreUrl);
                    try {
                        final ResponseEntity<String> responseEntity =
                                restTemplate.postForEntity(fullRestoreUrl, request, String.class);

                        if (responseEntity.getStatusCode() != HttpStatus.CREATED) {
                            errors.add("Failed to restore " + topologyType + " topology: "
                                    + responseEntity.getBody());
                        } else {
                            logger.info("Restored " + topologyType + " topology.");
                        }
                    } catch (RestClientException e) {
                        errors.add("POST to arangodb to restore topology failed with exception: " +
                                e.getLocalizedMessage());
                    }
                }
            });

            if (!errors.isEmpty()) {
                throw new DiagnosticsException(errors);
            }
        }

        @Nonnull
        @Override
        public byte[] dumpTopology(@Nonnull final TopologyID tid) throws DiagnosticsException {
            final List<String> errors = new ArrayList<>();
            final String db = tid.toDatabaseName();

            logger.info("Dumping real-time topology with database {}, {}", db, tid);

            byte[] retBytes = new byte[0];
            // Dump the specified topology from arangodb.
            try {
                final String fullDumpUrl = arangoDump.getEndpoint() + "/" + db;
                logger.info("Dumping topology database by calling {}", fullDumpUrl);
                final ResponseEntity<byte[]> databaseDumpEntity =
                        restTemplate.getForEntity(fullDumpUrl, byte[].class);
                if (databaseDumpEntity.getStatusCode() == HttpStatus.OK) {
                    logger.info("Finished dumping topology");
                    retBytes = databaseDumpEntity.getBody();
                } else {
                    errors.add("Fail to dump topology database: "
                            + new String(databaseDumpEntity.getBody()));
                }
            } catch (RestClientException e) {
                errors.add("Error retrieving ArangoDB dump from the remote service: " +
                        e.getLocalizedMessage());
            }

            if (!errors.isEmpty()) {
                throw new DiagnosticsException(errors);
            }

            return retBytes;
        }
    }
}
