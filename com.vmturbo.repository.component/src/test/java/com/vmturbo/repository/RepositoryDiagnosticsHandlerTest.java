package com.vmturbo.repository;

import static com.vmturbo.repository.RepositoryDiagnosticsHandler.ERRORS_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.ID_MGR_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.SUPPLY_CHAIN_RELATIONSHIP_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.TOPOLOGY_DUMP_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyIDManager;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;

public class RepositoryDiagnosticsHandlerTest {

    private ArangoDump arangoDump = mock(ArangoDump.class);
    private ArangoRestore arangoRestore = mock(ArangoRestore.class);
    private TopologyRelationshipRecorder relationshipRecorder =
            mock(TopologyRelationshipRecorder.class);
    private TopologyIDManager idManager = mock(TopologyIDManager.class);
    private RestTemplate restTemplate = mock(RestTemplate.class);
    private RecursiveZipReaderFactory zipReaderFactory = mock(RecursiveZipReaderFactory.class);
    private DiagnosticsWriter diagnosticsWriter = mock(DiagnosticsWriter.class);

    private final List<String> idMgrDiagLines = Collections.singletonList("idMgr");
    private final List<String> relationshipRecorderDiagLines = Collections.singletonList("rshp");
    private final byte[] dumpDiagBytes = new byte[]{1};
    private final String endpoint = "endpoint";
    private final String dbName = "db";
    private final String expectedUrl = endpoint + "/" + dbName;

    @Test
    public void testDumpNoRealtimeTopology() {
        when(idManager.getCurrentRealTimeTopologyId()).thenReturn(Optional.empty());

        RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        ZipOutputStream zos = mock(ZipOutputStream.class);
        List<String> errors = handler.dump(zos);
        assertEquals(1, errors.size());

        // Make sure the errors get written to the diags.
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), eq(errors), eq(zos));
    }

    @Test
    public void testDump() {
        setupDump();

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        final ZipOutputStream zos = mock(ZipOutputStream.class);
        handler.dump(zos);

        verify(diagnosticsWriter).writeZipEntry(eq(TOPOLOGY_DUMP_FILE), eq(dumpDiagBytes), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(SUPPLY_CHAIN_RELATIONSHIP_FILE),
                eq(relationshipRecorderDiagLines), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), eq(idMgrDiagLines), eq(zos));
        verify(diagnosticsWriter, times(0)).writeZipEntry(eq(ERRORS_FILE), any(List.class), any());
    }

    @Test
    public void testDumpBadStatusCode() {
        setupDump();

        final ResponseEntity<byte[]> dumpResponse = (ResponseEntity<byte[]>)mock(ResponseEntity.class);
        when(dumpResponse.getBody()).thenReturn(dumpDiagBytes);
        when(dumpResponse.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
        when(restTemplate.getForEntity(eq(expectedUrl), eq(byte[].class))).thenReturn(dumpResponse);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        final ZipOutputStream zos = mock(ZipOutputStream.class);

        final List<String> errors = handler.dump(zos);
        assertEquals(1, errors.size());

        // Topology Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
                .writeZipEntry(eq(TOPOLOGY_DUMP_FILE), any(byte[].class), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(SUPPLY_CHAIN_RELATIONSHIP_FILE),
                eq(relationshipRecorderDiagLines), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), eq(idMgrDiagLines), eq(zos));
        // Make sure the errors get written to the diags.
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), eq(errors), eq(zos));
    }

    @Test
    public void testDumpRestException() {
        setupDump();
        when(restTemplate.getForEntity(eq(expectedUrl), eq(byte[].class)))
                .thenThrow(RestClientException.class);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        final ZipOutputStream zos = mock(ZipOutputStream.class);

        final List<String> errors = handler.dump(zos);
        assertEquals(1, errors.size());

        // Topology Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
                .writeZipEntry(eq(TOPOLOGY_DUMP_FILE), any(byte[].class), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(SUPPLY_CHAIN_RELATIONSHIP_FILE),
                eq(relationshipRecorderDiagLines), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), eq(idMgrDiagLines), eq(zos));
        // Make sure the errors get written to the diags.
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), eq(errors), eq(zos));
    }

    @Test
    public void testRestore() {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        when(dumpDiags.getBytes()).thenReturn(dumpDiagBytes);

        setupRestore(idMgrDiags, rshpDiags, dumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        assertTrue(errors.isEmpty());
        verify(idManager).restoreDiags(eq(idMgrDiagLines));
        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(restTemplate).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreNoId() {
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        when(dumpDiags.getBytes()).thenReturn(dumpDiagBytes);

        setupRestore(rshpDiags, dumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        assertEquals(1, errors.size());

        // The relationships still get restored, but the topology dump doesn't get uploaded to
        // arangodb because there is no ID information.
        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(idManager, never()).restoreDiags(eq(idMgrDiagLines));
        verify(restTemplate, never()).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreNoTopologyDump() {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);

        setupRestore(idMgrDiags, rshpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        // There should be an error.
        assertEquals(1, errors.size());

        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(idManager).restoreDiags(eq(idMgrDiagLines));
        // There was no topology, so there shouldn't be a call to arango.
        verify(restTemplate, never()).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreNoTopologyDumpData() {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        // The file is there, but it doesn't have the right content.
        when(dumpDiags.getBytes()).thenReturn(null);

        setupRestore(idMgrDiags, rshpDiags, dumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        // There should be an error.
        assertEquals(1, errors.size());

        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(idManager).restoreDiags(eq(idMgrDiagLines));
        // There was no topology, so there shouldn't be a call to arango.
        verify(restTemplate, never()).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreWrongDiags() {
        // Set up all diags to return nulls for the data they're supposed to contain.
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(null);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(null);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        when(dumpDiags.getBytes()).thenReturn(null);

        setupRestore(idMgrDiags, rshpDiags, dumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        // 2 errors because of the nulls (the dump file doesn't get checked unless ID got restored),
        // and one error because the ID doesn't end up getting restored.
        assertEquals(3, errors.size());

        verify(idManager, never()).restoreDiags(eq(idMgrDiagLines));
        verify(relationshipRecorder, never()).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(restTemplate, never()).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreBadStatusCode() {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        when(dumpDiags.getBytes()).thenReturn(dumpDiagBytes);

        setupRestore(idMgrDiags, rshpDiags, dumpDiags);

        final ResponseEntity<String> restoreResponse =
                (ResponseEntity<String>)mock(ResponseEntity.class);
        when(restoreResponse.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
        when(restTemplate.postForEntity(eq(expectedUrl), any(), eq(String.class)))
                .thenReturn(restoreResponse);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        assertEquals(1, errors.size());

        verify(idManager).restoreDiags(eq(idMgrDiagLines));
        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(restTemplate).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    @Test
    public void testRestoreRestException() {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(SUPPLY_CHAIN_RELATIONSHIP_FILE);
        when(rshpDiags.getLines()).thenReturn(relationshipRecorderDiagLines);
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(TOPOLOGY_DUMP_FILE);
        when(dumpDiags.getBytes()).thenReturn(dumpDiagBytes);

        setupRestore(idMgrDiags, rshpDiags, dumpDiags);

        when(restTemplate.postForEntity(eq(expectedUrl), any(), eq(String.class)))
                .thenThrow(RestClientException.class);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(arangoDump, arangoRestore,
                        relationshipRecorder, idManager, restTemplate, zipReaderFactory,
                        diagnosticsWriter);
        List<String> errors = handler.restore(mock(InputStream.class));
        assertEquals(1, errors.size());

        verify(idManager).restoreDiags(eq(idMgrDiagLines));
        verify(relationshipRecorder).restoreDiags(eq(relationshipRecorderDiagLines));
        verify(restTemplate).postForEntity(eq(expectedUrl), any(), eq(String.class));
    }

    private void setupRestore(Diags diags, Diags... otherDiags) {
        final RecursiveZipReader zipReader = mock(RecursiveZipReader.class);
        final Iterator<Diags> diagsIt = (Iterator<Diags>)mock(Iterator.class);

        Boolean[] bools = new Boolean[otherDiags.length + 1];
        Arrays.fill(bools, true);
        if (bools.length > 0) {
            bools[bools.length - 1] = false;
        }

        when(diagsIt.hasNext()).thenReturn(true, bools);
        when(diagsIt.next()).thenReturn(diags, otherDiags);

        when(zipReader.iterator()).thenReturn(diagsIt);
        when(zipReaderFactory.createReader(any(InputStream.class))).thenReturn(zipReader);

        TopologyDatabase curDb = TopologyDatabase.from(dbName);
        when(idManager.currentRealTimeDatabase()).thenReturn(Optional.of(curDb));
        when(arangoRestore.getEndpoint()).thenReturn(endpoint);
        final ResponseEntity<String> restoreResponse =
                (ResponseEntity<String>)mock(ResponseEntity.class);
        when(restoreResponse.getStatusCode()).thenReturn(HttpStatus.CREATED);
        when(restTemplate.postForEntity(eq(expectedUrl), any(), eq(String.class)))
                .thenReturn(restoreResponse);
    }

    /**
     * Sets up the mocks for various calls required for the
     * {@link RepositoryDiagnosticsHandler#dump(ZipOutputStream)} method to work.
     * Tests that want to mess with certain subsets of the mocks (e.g. make the rest template
     * throw an exception) can do that setup after calling this method.
     */
    private void setupDump() {
        final ResponseEntity<byte[]> dumpResponse = (ResponseEntity<byte[]>)mock(ResponseEntity.class);
        when(dumpResponse.getBody()).thenReturn(dumpDiagBytes);
        when(dumpResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        when(restTemplate.getForEntity(eq(expectedUrl), eq(byte[].class))).thenReturn(dumpResponse);

        final TopologyID realtimeTopologyId = mock(TopologyID.class);
        when(idManager.getCurrentRealTimeTopologyId()).thenReturn(Optional.of(realtimeTopologyId));
        when(idManager.databaseName(eq(realtimeTopologyId))).thenReturn(dbName);
        when(arangoDump.getEndpoint()).thenReturn(endpoint);
        when(idManager.collectDiags()).thenReturn(idMgrDiagLines);
        when(relationshipRecorder.collectDiags()).thenReturn(relationshipRecorderDiagLines);
    }
}
