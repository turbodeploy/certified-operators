package com.vmturbo.api.component.diagnostics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.zip.ZipOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.api.component.diagnostics.ApiDiagnosticsHandler.VersionAndRevision;
import com.vmturbo.api.component.external.api.service.AdminService;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricUtilities;

public class ApiDiagnosticsHandlerTest {

    final SupplyChainFetcherFactory supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
    final SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder builder =
        Mockito.mock(SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder.class);
    final AdminService adminService = Mockito.mock(AdminService.class);
    final IClusterService clusterService = Mockito.mock(IClusterService.class);
    final DiagnosticsWriter diagnosticsWriter = Mockito.mock(DiagnosticsWriter.class);
    final ZipOutputStream diagnosticZip = Mockito.mock(ZipOutputStream.class);
    final long liveTopologyContextId = 123456L;

    final ApiDiagnosticsHandler diagnosticsHandler = Mockito.spy(
        new ApiDiagnosticsHandler(supplyChainFetcherFactory, adminService,
            clusterService, diagnosticsWriter, liveTopologyContextId));

    private static final String VERSION_STRING =
        "Turbonomic Operations Manager 7.4.0 (Build \"20180722153849000\") \"2018-07-24 12:26:58\"\n\n" +
        "mediation-vcenter: 7.4.0-SNAPSHOT\n" +
        "reporting: <unknown>\n" +
        "mediation-rhv: <unknown>\n" +
        "mediation-ucs: <unknown>\n" +
        "mediation-vplex: <unknown>\n" +
        "topology-processor: 7.4.0-SNAPSHOT\n" +
        "mediation-compellent: <unknown>\n" +
        "mediation-scaleio: <unknown>\n" +
        "mediation-netapp: <unknown>\n" +
        "mediation-vmax: <unknown>\n" +
        "repository: 7.4.0-SNAPSHOT\n" +
        "mediation-pure: <unknown>\n" +
        "market: <unknown>\n" +
        "mediation-hpe3par: <unknown>\n" +
        "action-orchestrator: <unknown>\n" +
        "mediation-xtremio: <unknown>\n" +
        "group: 7.4.0-SNAPSHOT\n" +
        "auth: 7.4.0-SNAPSHOT\n" +
        "mediation-vmm: <unknown>\n" +
        "history: <unknown>\n" +
        "plan-orchestrator: 7.4.0-SNAPSHOT\n" +
        "mediation-hyperv: <unknown>\n" +
        "api: 7.4.0-SNAPSHOT\n" +
        "mediation-hds: <unknown>\n" +
        "mediation-openstack: <unknown>";

    @Before
    public void setup() throws Exception {
        CollectorRegistry.defaultRegistry.clear();

        when(supplyChainFetcherFactory.newApiDtoFetcher()).thenReturn(builder);
        when(builder.topologyContextId(eq(liveTopologyContextId))).thenReturn(builder);
        when(builder.addSeedUuids(eq(Collections.emptyList()))).thenReturn(builder);
        when(builder.entityDetailType(eq(null))).thenReturn(builder);
        when(builder.includeHealthSummary(eq(false))).thenReturn(builder);

        final SupplychainApiDTO supplyChain = new SupplychainApiDTO();
        supplyChain.setSeMap(ImmutableMap.of(
            "VirtualMachine", supplyChainEntry(21),
            "PhysicalMachine", supplyChainEntry(4)
        ));

        doReturn(supplyChain).when(builder).fetch();
    }

    @After
    public void teardown() {
        CollectorRegistry.defaultRegistry.clear();
    }

    @Test
    public void testCollectTelemetryWhenEnabled() {
        when(clusterService.isTelemetryEnabled()).thenReturn(true);
        final ProductVersionDTO versionDTO = new ProductVersionDTO();
        versionDTO.setMarketVersion(2);
        versionDTO.setUpdates("");
        versionDTO.setVersionInfo(VERSION_STRING);
        when(adminService.getVersionInfo(eq(true))).thenReturn(versionDTO);

        diagnosticsHandler.dump(diagnosticZip);
        verify(diagnosticsWriter).writePrometheusMetrics(
            eq(CollectorRegistry.defaultRegistry), eq(diagnosticZip));
    }

    @Test
    public void testDoNotCollectTelemetryWhenDisabled() {
        when(clusterService.isTelemetryEnabled()).thenReturn(false);

        diagnosticsHandler.dump(diagnosticZip);
        verify(diagnosticsWriter, never()).writePrometheusMetrics(
            eq(CollectorRegistry.defaultRegistry), eq(diagnosticZip));
    }

    @Test
    public void testParseVersionAndRevision() {
        final VersionAndRevision versionAndRevision =
            new VersionAndRevision(VERSION_STRING);

        assertEquals("Turbonomic Operations Manager 7.4.0", versionAndRevision.version);
        assertEquals("20180722153849000", versionAndRevision.revision);
    }

    @Test
    public void testParseVersionAndRevisionError() {
        final ProductVersionDTO versionDTO = new ProductVersionDTO();

        final VersionAndRevision versionAndRevision = new VersionAndRevision(versionDTO.getVersionInfo());
        assertEquals("", versionAndRevision.version);
        assertEquals("", versionAndRevision.revision);
    }

    @Test
    public void testCollectEntityCounts() throws Exception {
        diagnosticsHandler.collectEntityCounts();

        final String metrics = TelemetryMetricUtilities.format004(CollectorRegistry.defaultRegistry);
        assertTrue(metrics.contains("{service_entity_name=\"VirtualMachine\",} 21.0"));
        assertTrue(metrics.contains("{service_entity_name=\"PhysicalMachine\",} 4.0"));
    }

    private SupplychainEntryDTO supplyChainEntry(final int entityCount) {
        final  SupplychainEntryDTO entryDTO = new SupplychainEntryDTO();
        entryDTO.setEntitiesCount(entityCount);

        return entryDTO;
    }
}