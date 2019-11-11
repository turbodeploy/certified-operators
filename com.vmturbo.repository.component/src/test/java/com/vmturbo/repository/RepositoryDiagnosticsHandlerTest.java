package com.vmturbo.repository;

import static com.vmturbo.repository.RepositoryDiagnosticsHandler.ERRORS_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.GLOBAL_SUPPLY_CHAIN_DIAGS_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.ID_MGR_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.PROJECTED_TOPOLOGY_DUMP_FILE;
import static com.vmturbo.repository.RepositoryDiagnosticsHandler.SOURCE_TOPOLOGY_DUMP_FILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import io.prometheus.client.CollectorRegistry;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.repository.RepositoryDiagnosticsHandler.DefaultTopologyDiagnostics;
import com.vmturbo.repository.RepositoryDiagnosticsHandler.TopologyDiagnostics;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.topology.GlobalSupplyChain;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

public class RepositoryDiagnosticsHandlerTest {

    private TopologyLifecycleManager lifecycleManager = mock(TopologyLifecycleManager.class);

    private DiagsZipReaderFactory zipReaderFactory = mock(DiagsZipReaderFactory.class);

    private GlobalSupplyChainManager globalSupplyChainManager =
            mock(GlobalSupplyChainManager.class);

    private GraphDBExecutor graphDBExecutor = mock(GraphDBExecutor.class);

    private DiagnosticsWriter diagnosticsWriter = mock(DiagnosticsWriter.class);

    private TopologyDiagnostics topologyDiagnostics = mock(TopologyDiagnostics.class);

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private ArangoDump arangoDump = mock(ArangoDump.class);

    private ArangoRestore arangoRestore = mock(ArangoRestore.class);

    private RestTemplate restTemplate = mock(RestTemplate.class);

    private final List<String> idMgrDiagLines = Collections.singletonList("idMgr");
    private final Stream<String> idMgrDiagLinesStream = idMgrDiagLines.stream();
    private final List<String> globalSupplyChainOutput =
            Collections.singletonList("global-supply-chain");
    private final Stream<String> globalSupplyChainOutputStream = globalSupplyChainOutput.stream();
    private final String endpoint = "endpoint";
    private final String dbName = "db";
    private final String expectedUrl = endpoint + "/" + dbName;

    private final byte[] sourceTopoDump = new byte[]{1};
    private final byte[] projectedTopoDump = new byte[]{2};

    @Test
    public void testDumpNoRealtimeTopology() throws DiagnosticsException {
        when(lifecycleManager.getRealtimeTopologyId(any()))
                .thenReturn(Optional.empty());
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        when(liveTopologyStore.getProjectedTopology()).thenReturn(Optional.empty());

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager, liveTopologyStore,
                        zipReaderFactory, diagnosticsWriter,
                        graphDBExecutor, topologyDiagnostics);
        ZipOutputStream zos = mock(ZipOutputStream.class);
        List<String> errors = handler.dump(zos);
        // One because source topology wasn't found, one because projected topology wasn't found.
        assertEquals(2, errors.size());

        // Make sure the errors get written to the diags.
        ArgumentCaptor<Stream> errorStreamCaptor = ArgumentCaptor.forClass(Stream.class);
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), errorStreamCaptor.capture(), eq(zos));
        assertEquals(errors, errorStreamCaptor.getValue().collect(Collectors.toList()));
    }
    @Test
    public void testDump() throws DiagnosticsException {
        setupDump();

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                    liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                    graphDBExecutor, topologyDiagnostics);
        final ZipOutputStream zos = mock(ZipOutputStream.class);

        handler.dump(zos);

        verify(diagnosticsWriter).writeZipEntry(eq(SOURCE_TOPOLOGY_DUMP_FILE),
                eq(sourceTopoDump), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(PROJECTED_TOPOLOGY_DUMP_FILE),
                eq(projectedTopoDump), eq(zos));
        ArgumentCaptor<Stream> streamCaptor = ArgumentCaptor.forClass(Stream.class);
        verify(diagnosticsWriter).writeZipEntry(eq(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE),
            streamCaptor.capture(), eq(zos));
        assertEquals(globalSupplyChainOutput, streamCaptor.getValue().collect(Collectors.toList()));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), streamCaptor.capture(), eq(zos));
        assertEquals(idMgrDiagLines, streamCaptor.getValue().collect(Collectors.toList()));
        verify(diagnosticsWriter).writePrometheusMetrics(any(CollectorRegistry.class), eq(zos));
        verify(diagnosticsWriter, times(0)).writeZipEntry(eq(ERRORS_FILE), any(Stream.class), any());
    }

    @Test
    public void testDumpSourceException() throws DiagnosticsException {
        setupDump();

        final TopologyID sourceTopologyId = mock(TopologyID.class);
        when(lifecycleManager.getRealtimeTopologyId(eq(TopologyType.SOURCE)))
               .thenReturn(Optional.of(sourceTopologyId));
        when(topologyDiagnostics.dumpTopology(eq(sourceTopologyId)))
            .thenThrow(new DiagnosticsException(Collections.singletonList("ERROR")));

        GlobalSupplyChain globalSupplyChain = mock(GlobalSupplyChain.class);
        when(globalSupplyChainManager.getGlobalSupplyChain(sourceTopologyId))
                .thenReturn(Optional.of(globalSupplyChain));

        when(globalSupplyChain.collectDiagsStream()).thenReturn(globalSupplyChainOutputStream);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                    liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                    graphDBExecutor, topologyDiagnostics);
        final ZipOutputStream zos = mock(ZipOutputStream.class);

        final List<String> errors = handler.dump(zos);
        assertThat(errors, containsInAnyOrder("ERROR"));

        // Topology Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
                .writeZipEntry(eq(SOURCE_TOPOLOGY_DUMP_FILE), any(byte[].class), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(PROJECTED_TOPOLOGY_DUMP_FILE),
                eq(projectedTopoDump), eq(zos));
        ArgumentCaptor<Stream> streamCaptor = ArgumentCaptor.forClass(Stream.class);
        verify(diagnosticsWriter).writeZipEntry(eq(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE),
            streamCaptor.capture(), eq(zos));
        assertEquals(globalSupplyChainOutput, streamCaptor.getValue().collect(Collectors.toList()));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), streamCaptor.capture(), eq(zos));
        assertEquals(idMgrDiagLines, streamCaptor.getValue().collect(Collectors.toList()));
        // Make sure the errors get written to the diags.
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), streamCaptor.capture(), eq(zos));
        assertEquals(errors, streamCaptor.getValue().collect(Collectors.toList()));
    }

    @Test
    public void testDumpProjectedException() throws DiagnosticsException {
        setupDump();

        final TopologyID projectedTopologyId = mock(TopologyID.class);
        when(lifecycleManager.getRealtimeTopologyId(eq(TopologyType.PROJECTED)))
                .thenReturn(Optional.of(projectedTopologyId));
        when(topologyDiagnostics.dumpTopology(eq(projectedTopologyId)))
                .thenThrow(new DiagnosticsException(Collections.singletonList("ERROR")));

        final RepositoryDiagnosticsHandler handler =
            new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                graphDBExecutor, topologyDiagnostics);
        final ZipOutputStream zos = mock(ZipOutputStream.class);

        final List<String> errors = handler.dump(zos);
        assertEquals(1, errors.size());

        // Topology Dump file shouldn't get written.
        verify(diagnosticsWriter, never())
                .writeZipEntry(eq(PROJECTED_TOPOLOGY_DUMP_FILE), any(byte[].class), eq(zos));
        verify(diagnosticsWriter).writeZipEntry(eq(SOURCE_TOPOLOGY_DUMP_FILE),
                eq(sourceTopoDump), eq(zos));
        ArgumentCaptor<Stream> streamCaptor = ArgumentCaptor.forClass(Stream.class);
        verify(diagnosticsWriter).writeZipEntry(eq(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE),
            streamCaptor.capture(), eq(zos));
        assertEquals(globalSupplyChainOutput, streamCaptor.getValue().collect(Collectors.toList()));
        verify(diagnosticsWriter).writeZipEntry(eq(ID_MGR_FILE), streamCaptor.capture(), eq(zos));
        assertEquals(idMgrDiagLines, streamCaptor.getValue().collect(Collectors.toList()));
        // Make sure the errors get written to the diags.
        verify(diagnosticsWriter).writeZipEntry(eq(ERRORS_FILE), streamCaptor.capture(), eq(zos));
        assertEquals(errors, streamCaptor.getValue().collect(Collectors.toList()));
    }

    @Test
    public void testRestore() throws DiagnosticsException {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        when(rshpDiags.getLines()).thenReturn(globalSupplyChainOutput);
        final Diags srcDumpDiags = mock(Diags.class);
        when(srcDumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        final Diags projectedDumpDiags = mock(Diags.class);
        when(projectedDumpDiags.getName()).thenReturn(PROJECTED_TOPOLOGY_DUMP_FILE);

        setupRestore(idMgrDiags, rshpDiags, srcDumpDiags, projectedDumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                        liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                        graphDBExecutor, topologyDiagnostics);
        when(lifecycleManager.getRealtimeTopologyId()).thenReturn(Optional.empty());
        List<String> errors = handler.restore(mock(InputStream.class));
        assertTrue(errors.isEmpty());
        verify(lifecycleManager).restoreDiags(eq(idMgrDiagLines));
        verify(topologyDiagnostics).restoreTopology(eq(Optional.of(srcDumpDiags)),
                eq(TopologyType.SOURCE));
        verify(topologyDiagnostics).restoreTopology(eq(Optional.of(projectedDumpDiags)),
                eq(TopologyType.PROJECTED));
    }

    @Test
    public void testRestoreNoId() throws DiagnosticsException {
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        when(rshpDiags.getLines()).thenReturn(globalSupplyChainOutput);
        final Diags srcDumpDiags = mock(Diags.class);
        when(srcDumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        final Diags projectedDumpDiags = mock(Diags.class);
        when(projectedDumpDiags.getName()).thenReturn(PROJECTED_TOPOLOGY_DUMP_FILE);
        when(lifecycleManager.getRealtimeTopologyId()).thenReturn(Optional.empty());
        setupRestore(rshpDiags, srcDumpDiags, projectedDumpDiags);

        final RepositoryDiagnosticsHandler handler =
            new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager, liveTopologyStore,
                    zipReaderFactory, diagnosticsWriter,
                    graphDBExecutor, topologyDiagnostics);
        List<String> errors = handler.restore(mock(InputStream.class));
        assertEquals(1, errors.size());

        // The relationships still get restored, but the topology dump doesn't get uploaded to
        // arangodb because there is no ID information.
        verify(lifecycleManager, never()).restoreDiags(eq(idMgrDiagLines));
        verify(topologyDiagnostics, never()).restoreTopology(any(), any());
    }

    @Test
    public void testRestoreSourceException() throws DiagnosticsException {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        when(rshpDiags.getLines()).thenReturn(globalSupplyChainOutput);
        final Diags projectedDumpDiags = mock(Diags.class);
        when(projectedDumpDiags.getName()).thenReturn(PROJECTED_TOPOLOGY_DUMP_FILE);

        when(lifecycleManager.getRealtimeTopologyId()).thenReturn(Optional.empty());

        // Restoring source topology fails.
        final Diags srcDumpDiags = mock(Diags.class);
        when(srcDumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        doThrow(new DiagnosticsException(Collections.singletonList("ERROR")))
            .when(topologyDiagnostics).restoreTopology(eq(Optional.of(srcDumpDiags)),
                eq(TopologyType.SOURCE));


        setupRestore(idMgrDiags, rshpDiags, srcDumpDiags, projectedDumpDiags);

        final RepositoryDiagnosticsHandler handler =
            new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                graphDBExecutor, topologyDiagnostics);

        List<String> errors = handler.restore(mock(InputStream.class));
        assertThat(errors, containsInAnyOrder("ERROR"));

        verify(lifecycleManager).restoreDiags(eq(idMgrDiagLines));
        // Make sure it still tries to restore the projected topology.
        verify(topologyDiagnostics).restoreTopology(eq(Optional.of(projectedDumpDiags)),
                eq(TopologyType.PROJECTED));
    }

    @Test
    public void testRestoreProjectedException() throws DiagnosticsException {
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(idMgrDiagLines);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        when(rshpDiags.getLines()).thenReturn(globalSupplyChainOutput);
        final Diags srcDumpDiags = mock(Diags.class);
        when(srcDumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        when(lifecycleManager.getRealtimeTopologyId()).thenReturn(Optional.empty());
        // Restoring projected topology fails.
        final Diags projectedDumpDiags = mock(Diags.class);
        when(projectedDumpDiags.getName()).thenReturn(PROJECTED_TOPOLOGY_DUMP_FILE);
        doThrow(new DiagnosticsException(Collections.singletonList("ERROR")))
                .when(topologyDiagnostics).restoreTopology(eq(Optional.of(projectedDumpDiags)),
                eq(TopologyType.PROJECTED));


        setupRestore(idMgrDiags, rshpDiags, srcDumpDiags, projectedDumpDiags);

        final RepositoryDiagnosticsHandler handler =
                new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                        liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                        graphDBExecutor, topologyDiagnostics);

        List<String> errors = handler.restore(mock(InputStream.class));
        assertThat(errors, containsInAnyOrder("ERROR"));

        verify(lifecycleManager).restoreDiags(eq(idMgrDiagLines));
        // Make sure it still tries to restore the source topology.
        verify(topologyDiagnostics).restoreTopology(eq(Optional.of(srcDumpDiags)),
                eq(TopologyType.SOURCE));
    }

    @Test(expected = DiagnosticsException.class)
    public void testRestoreTopologyNoDiags() throws DiagnosticsException {
        final DefaultTopologyDiagnostics topologyDiagnostics =
                new DefaultTopologyDiagnostics(arangoDump, arangoRestore, lifecycleManager,
                        restTemplate);
        topologyDiagnostics.restoreTopology(Optional.empty(), TopologyType.SOURCE);
    }

    @Test(expected = DiagnosticsException.class)
    public void testRestoreNoTopologyDumpData() throws DiagnosticsException {
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        // The file is there, but it doesn't have the right content.
        when(dumpDiags.getBytes()).thenReturn(null);

        final DefaultTopologyDiagnostics topologyDiagnostics =
                new DefaultTopologyDiagnostics(arangoDump, arangoRestore, lifecycleManager,
                        restTemplate);
        topologyDiagnostics.restoreTopology(Optional.of(dumpDiags), TopologyType.SOURCE);
    }

    @Test(expected = DiagnosticsException.class)
    public void testRestoreTopologyRestException() throws DiagnosticsException {
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        // The file is there, but it doesn't have the right content.
        when(dumpDiags.getBytes()).thenReturn(sourceTopoDump);

        final DefaultTopologyDiagnostics topologyDiagnostics =
                new DefaultTopologyDiagnostics(arangoDump, arangoRestore, lifecycleManager,
                        restTemplate);

        final TopologyID tid = mock(TopologyID.class);
        when(tid.toDatabaseName()).thenReturn(dbName);
        when(lifecycleManager.getRealtimeTopologyId(TopologyType.SOURCE))
            .thenReturn(Optional.of(tid));
        when(arangoRestore.getEndpoint()).thenReturn(endpoint);
        when(restTemplate.postForEntity(eq(expectedUrl), any(), any()))
            .thenThrow(RestClientException.class);
        topologyDiagnostics.restoreTopology(Optional.of(dumpDiags), TopologyType.SOURCE);
    }

    @Test(expected = DiagnosticsException.class)
    public void testRestoreTopologyWrongStatusCode() throws DiagnosticsException {
        final Diags dumpDiags = mock(Diags.class);
        when(dumpDiags.getName()).thenReturn(SOURCE_TOPOLOGY_DUMP_FILE);
        // The file is there, but it doesn't have the right content.
        when(dumpDiags.getBytes()).thenReturn(sourceTopoDump);

        final DefaultTopologyDiagnostics topologyDiagnostics =
                new DefaultTopologyDiagnostics(arangoDump, arangoRestore, lifecycleManager,
                        restTemplate);

        final TopologyID tid = mock(TopologyID.class);
        when(tid.toDatabaseName()).thenReturn(dbName);
        when(lifecycleManager.getRealtimeTopologyId(TopologyType.SOURCE))
                .thenReturn(Optional.of(tid));
        when(arangoRestore.getEndpoint()).thenReturn(endpoint);

        final ResponseEntity<String> responseEntity =
                (ResponseEntity<String>)mock(ResponseEntity.class);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.BAD_REQUEST);
        when(responseEntity.getBody()).thenReturn("");

        when(restTemplate.<String>postForEntity(eq(expectedUrl), any(), any()))
                .thenReturn(responseEntity);

        topologyDiagnostics.restoreTopology(Optional.of(dumpDiags), TopologyType.SOURCE);
    }

    @Test
    public void testRestoreWrongDiags() throws DiagnosticsException {
        // Set up all diags to return nulls for the data they're supposed to contain.
        final Diags idMgrDiags = mock(Diags.class);
        when(idMgrDiags.getName()).thenReturn(ID_MGR_FILE);
        when(idMgrDiags.getLines()).thenReturn(null);
        final Diags rshpDiags = mock(Diags.class);
        when(rshpDiags.getName()).thenReturn(GLOBAL_SUPPLY_CHAIN_DIAGS_FILE);
        when(rshpDiags.getLines()).thenReturn(null);

        // The topology dumps get handled inside the TopologyDiagnostics interface, so ignore them.

        setupRestore(idMgrDiags, rshpDiags);

        final RepositoryDiagnosticsHandler handler =
            new RepositoryDiagnosticsHandler(globalSupplyChainManager, lifecycleManager,
                liveTopologyStore, zipReaderFactory, diagnosticsWriter,
                graphDBExecutor, topologyDiagnostics);
        List<String> errors = handler.restore(mock(InputStream.class));
        // 2 errors because of the nulls (the dump file doesn't get checked unless ID got restored),
        // and one error because the ID doesn't end up getting restored.
        assertEquals(3, errors.size());

        verify(lifecycleManager, never()).restoreDiags(eq(idMgrDiagLines));
    }

    private void setupRestore(Diags diags, Diags... otherDiags) {
        final DiagsZipReader zipReader = mock(DiagsZipReader.class);
        final Iterator<Diags> diagsIt = (Iterator<Diags>)mock(Iterator.class);

        Boolean[] bools = new Boolean[otherDiags.length + 1];
        Arrays.fill(bools, true);
        bools[bools.length - 1] = false;

        when(diagsIt.hasNext()).thenReturn(true, bools);
        when(diagsIt.next()).thenReturn(diags, otherDiags);

        when(zipReader.iterator()).thenReturn(diagsIt);
        when(zipReaderFactory.createReader(any(InputStream.class))).thenReturn(zipReader);

        TopologyDatabase curDb = TopologyDatabase.from(dbName);
        when(lifecycleManager.getRealtimeDatabase()).thenReturn(Optional.of(curDb));
        when(arangoRestore.getEndpoint()).thenReturn(endpoint);
        final ResponseEntity<String> restoreResponse =
                (ResponseEntity<String>)mock(ResponseEntity.class);
        when(restoreResponse.getStatusCode()).thenReturn(HttpStatus.CREATED);
        when(restTemplate.postForEntity(eq(expectedUrl), any(), eq(String.class)))
                .thenReturn(restoreResponse);

        final TopologyID sourceTopologyId = mock(TopologyID.class);
        when(lifecycleManager.getRealtimeTopologyId(eq(TopologyType.SOURCE)))
                .thenReturn(Optional.of(sourceTopologyId));

        GlobalSupplyChain globalSupplyChain = mock(GlobalSupplyChain.class);
        when(globalSupplyChainManager.getGlobalSupplyChain(sourceTopologyId))
                .thenReturn(Optional.of(globalSupplyChain));
    }

    /**
     * Sets up the mocks for various calls required for the
     * {@link RepositoryDiagnosticsHandler#dump(ZipOutputStream)} method to work.
     * Tests that want to mess with certain subsets of the mocks (e.g. make the rest template
     * throw an exception) can do that setup after calling this method.
     */
    private void setupDump() throws DiagnosticsException {

        final TopologyID sourceTopologyId = mock(TopologyID.class);
        when(lifecycleManager.getRealtimeTopologyId(eq(TopologyType.SOURCE)))
                .thenReturn(Optional.of(sourceTopologyId));

        final TopologyID projectedTopologyId = mock(TopologyID.class);
        when(lifecycleManager.getRealtimeTopologyId(eq(TopologyType.PROJECTED)))
                .thenReturn(Optional.of(projectedTopologyId));

        GlobalSupplyChain globalSupplyChain = mock(GlobalSupplyChain.class);
        when(globalSupplyChainManager.getGlobalSupplyChain(sourceTopologyId))
                .thenReturn(Optional.of(globalSupplyChain));

        when(globalSupplyChain.collectDiagsStream()).thenReturn(globalSupplyChainOutputStream);
        when(topologyDiagnostics.dumpTopology(eq(sourceTopologyId))).thenReturn(sourceTopoDump);
        when(topologyDiagnostics.dumpTopology(eq(projectedTopologyId))).thenReturn(projectedTopoDump);

        when(lifecycleManager.collectDiagsStream()).thenReturn(idMgrDiagLinesStream);

        SourceRealtimeTopology sourceRealtimeTopology = mock(SourceRealtimeTopology.class);
        ProjectedRealtimeTopology projectedRealtimeTopology = mock(ProjectedRealtimeTopology.class);
        when(sourceRealtimeTopology.collectDiags()).thenReturn(Stream.of("Foo"));
        when(projectedRealtimeTopology.collectDiags()).thenReturn(Stream.of("Boo"));
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(sourceRealtimeTopology));
        when(liveTopologyStore.getProjectedTopology()).thenReturn(Optional.of(projectedRealtimeTopology));
    }
}
