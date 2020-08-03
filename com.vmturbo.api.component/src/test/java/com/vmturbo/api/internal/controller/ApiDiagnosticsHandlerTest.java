package com.vmturbo.api.internal.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.service.AdminService;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.internal.controller.VersionDiagnosable.VersionAndRevision;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.proactivesupport.metrics.TelemetryMetricUtilities;

/**
 * Test handling for the Api Diagnostic Dump functions.
 */
public class ApiDiagnosticsHandlerTest {

    private final SupplyChainFetcherFactory supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
    private final SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder builder =
        Mockito.mock(SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder.class);
    private final AdminService adminService = Mockito.mock(AdminService.class);
    private final ClusterMgrRestClient clusterService = Mockito.mock(ClusterMgrRestClient.class);
    private final ZipOutputStream diagnosticZip = Mockito.mock(ZipOutputStream.class);
    private final long liveTopologyContextId = 123456L;

    private DiagnosticsHandler diagnosticsHandler;
    private VersionDiagnosable versionDiagsProvider;
    private PrometheusDiagnosticsProvider prometheusDiagnosticsProvider;

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
        versionDiagsProvider =
                new VersionDiagnosable(supplyChainFetcherFactory, adminService, clusterService,
                        liveTopologyContextId);
        prometheusDiagnosticsProvider =
                Mockito.spy(new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry));
        diagnosticsHandler = new DiagnosticsHandler(
                Lists.newArrayList(versionDiagsProvider, prometheusDiagnosticsProvider));
        CollectorRegistry.defaultRegistry.clear();

        when(supplyChainFetcherFactory.newApiDtoFetcher()).thenReturn(builder);
        when(builder.topologyContextId(eq(liveTopologyContextId))).thenReturn(builder);
        when(builder.addSeedUuids(eq(Collections.emptyList()))).thenReturn(builder);
        when(builder.entityDetailType(eq(null))).thenReturn(builder);
        when(builder.includeHealthSummary(eq(false))).thenReturn(builder);
        Mockito.when(adminService.getVersionInfo(Mockito.anyBoolean()))
                .thenReturn(new ProductVersionDTO());

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
    public void testCollectTelemetryWhenEnabled() throws DiagnosticsException {
        when(clusterService.isTelemetryEnabled()).thenReturn(true);
        final ProductVersionDTO versionDTO = new ProductVersionDTO();
        versionDTO.setMarketVersion(2);
        versionDTO.setUpdates("");
        versionDTO.setVersionInfo(VERSION_STRING);
        when(adminService.getVersionInfo(eq(true))).thenReturn(versionDTO);

        diagnosticsHandler.dump(diagnosticZip);
        Mockito.verify(prometheusDiagnosticsProvider).collectDiags(Mockito.any());
    }

    @Test
    public void testDoNotCollectTelemetryWhenDisabled() throws DiagnosticsException {
        when(clusterService.isTelemetryEnabled()).thenReturn(false);

        diagnosticsHandler.dump(diagnosticZip);
        Mockito.verify(prometheusDiagnosticsProvider).collectDiags(Mockito.any());
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
        final VersionAndRevision versionAndRevision =
            new VersionAndRevision(VERSION_STRING);
        versionDiagsProvider.collectEntityCounts(versionAndRevision);

        final String metrics = TelemetryMetricUtilities.format004(CollectorRegistry.defaultRegistry);
        assertTrue(metrics.contains("service_entity_count{service_entity_name=\"VirtualMachine\",target_type=\"\",version=\"Turbonomic Operations Manager 7.4.0:20180722153849000\",} 21.0"));
        assertTrue(metrics.contains("service_entity_count{service_entity_name=\"PhysicalMachine\",target_type=\"\",version=\"Turbonomic Operations Manager 7.4.0:20180722153849000\",} 4.0"));
    }

    private SupplychainEntryDTO supplyChainEntry(final int entityCount) {
        final  SupplychainEntryDTO entryDTO = new SupplychainEntryDTO();
        entryDTO.setEntitiesCount(entityCount);

        return entryDTO;
    }
}
