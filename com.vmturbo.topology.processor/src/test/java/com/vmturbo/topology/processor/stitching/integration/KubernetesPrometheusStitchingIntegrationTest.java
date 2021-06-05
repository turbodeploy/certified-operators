package com.vmturbo.topology.processor.stitching.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.stitching.serviceslo.ServiceSLOStitchingOperation;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Integration test for the stitching between Kubernetes and Prometheus.
 */
public class KubernetesPrometheusStitchingIntegrationTest extends StitchingIntegrationTest {

    private StitchingManager stitchingManager;

    private static final long kubernetesTargetId = 1111L;
    private static final long prometheusTargetId = 2222L;
    private static final long kubernetesProbeId = 2333L;
    private static final long prometheusProbeId = 6666L;

    private static final long oid_service = 11L;
    private static final long oid_app1 = 21L;
    private static final long oid_app1_sidecar = 22L;
    private static final long oid_app2 = 31L;
    private static final long oid_app2_sidecar = 33L;

    private static final long oid_service1_proxy = 111L;
    private static final long oid_app1_proxy = 211L;
    private static final long oid_service2_proxy = 112L;
    private static final long oid_app2_proxy = 212L;

    // kubernetes entities only trade keyed Application commodities, but don't trade SLO commodities
    private final EntityDTO app1 = EntityBuilders.applicationComponent("app1")
        .selling(CommodityBuilders.application().sold().key("d74685aa-8b98-4e02-a95f-82da8e84c934"))
        .property("IP", "10.2.1.31")
        .build();

    private final EntityDTO app1_sidecar = EntityBuilders.applicationComponent("app1-sidecar")
        .selling(CommodityBuilders.application().sold().key("d74685aa-8b98-4e02-a95f-82da8e84c934-1"))
        .property("IP", "10.2.1.31-1")
        .build();

    private final EntityDTO app2 = EntityBuilders.applicationComponent("app2")
            .selling(CommodityBuilders.application().sold().key("d74685aa-8b98-4e02-a95f-82da8e84c934"))
            .property("IP", "10.2.1.32")
            .build();

    private final EntityDTO app2_sidecar = EntityBuilders.applicationComponent("app2-sidecar")
            .selling(CommodityBuilders.application().sold().key("d74685aa-8b98-4e02-a95f-82da8e84c934-1"))
            .property("IP", "10.2.1.32-1")
            .build();

    // prometheus proxy entities trade non-keyed SLO commodities and keyed Application commodities
    private final EntityDTO service1_proxy = EntityBuilders.service("service1-proxy")
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-proxy").used(12d))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-proxy").used(10d))
        .buying(CommodityBuilders.application().from("app1-proxy").key("Service-10.2.1.31-demoapp"))
        .selling(CommodityBuilders.transactionsPerSecond().sold().used(12d))
        .selling(CommodityBuilders.responseTimeMillis().sold().used(10d))
        .property("IP", "service-10.2.1.31")
        .proxy()
        .build();

    private final EntityDTO app1_proxy = EntityBuilders.applicationComponent("app1-proxy")
        .selling(CommodityBuilders.transactionsPerSecond().sold().used(12d))
        .selling(CommodityBuilders.responseTimeMillis().sold().used(10d))
        .selling(CommodityBuilders.application().sold().key("Service-10.2.1.31-demoapp"))
        .property("IP", "10.2.1.31")
        .proxy()
        .build();

    private final EntityDTO service2_proxy = EntityBuilders.service("service2-proxy")
        .buying(CommodityBuilders.transactionsPerSecond().from("app2-proxy").used(15d))
        .buying(CommodityBuilders.responseTimeMillis().from("app2-proxy").used(8d))
        .buying(CommodityBuilders.application().from("app2-proxy").key("Service-10.2.1.32-demoapp"))
        .selling(CommodityBuilders.transactionsPerSecond().sold().used(15d))
        .selling(CommodityBuilders.responseTimeMillis().sold().used(8d))
        .property("IP", "service-10.2.1.32")
        .proxy()
        .build();

    private final EntityDTO app2_proxy = EntityBuilders.applicationComponent("app2-proxy")
        .selling(CommodityBuilders.transactionsPerSecond().sold().used(15d))
        .selling(CommodityBuilders.responseTimeMillis().sold().used(8d))
        .selling(CommodityBuilders.application().sold().key("Service-10.2.1.32-demoapp"))
        .property("IP", "10.2.1.32")
        .proxy()
        .build();

    /**
     * Test the stitching between Kubernetes and Prometheus. Currently Prometheus returns proxy
     * service and app in pairs, if in Kubernetes the service consumes two apps, then Prometheus returns
     * two pairs of service.
     *
     * The EntityDTOs used in the test will be the following:
     *
     * Before stitching:
     *
     *     Kubernetes           Prometheus
     *
     *      service1          service1-proxy
     *       /   \                  |
     *    app1  app1-sidecar    app1-proxy
     *
     * After stitching:
     *
     *     Kubernetes
     *
     *       service1          (bought commodities got used value from Prometheus)
     *       /   \
     *    app1  app1-sidecar   (sold commodities got used value from Prometheus)
     *
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testPrometheusOneServiceBuyingFromOneApp() throws Exception {
        init(getDataDrivenStitchingOperations());
        final EntityDTO service = EntityBuilders.service("service")
            .buying(CommodityBuilders.application()
                    .from("app1")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934"))
            .buying(CommodityBuilders.application()
                    .from("app1-sidecar")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934-1"))
            .property("IP", "service-10.2.1.31")
            .build();
        final Map<Long, EntityDTO> kubernetesEntities = ImmutableMap.of(
                oid_service, service,
                oid_app1, app1,
                oid_app1_sidecar, app1_sidecar
        );
        final Map<Long, EntityDTO> prometheusEntities = ImmutableMap.of(
                oid_service1_proxy, service1_proxy,
                oid_app1_proxy, app1_proxy
        );
        addEntities(kubernetesEntities, kubernetesTargetId);
        addEntities(prometheusEntities, prometheusTargetId);

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();

        // verify that there are 5 entities before stitching
        assertEquals(5, stitchingContext.getStitchingGraph().entityCount());

        // stitch
        Map<Long, TopologyEntityDTO.Builder> topology = stitch(stitchingContext);

        // verify that only entities from Kubernetes are left after stitching
        assertEquals(3, topology.size());

        final TopologyEntityDTO.Builder svc = topology.get(oid_service);
        final TopologyEntityDTO.Builder app1 = topology.get(oid_app1);

        // verify that service buys commodities from app1 with used value patched from prometheus
        final CommoditiesBoughtFromProvider cb1 = verifyAndGetCommoditiesBought(svc, app1);
        assertEquals(3, cb1.getCommodityBoughtCount());
        verifyBuyingCommodity(cb1, CommodityType.TRANSACTION_VALUE, "", 12d);
        verifyBuyingCommodity(cb1, CommodityType.RESPONSE_TIME_VALUE, "", 10d);
        verifyBuyingCommodity(cb1, CommodityType.APPLICATION_VALUE,
                "d74685aa-8b98-4e02-a95f-82da8e84c934", 0d);
    }

    /**
     * Test the stitching between Kubernetes and Prometheus, and verify that after stitching, the
     * used value of sold commodities of service are aggregated
     *
     * The EntityDTOs used in the test will be the following:
     *
     * Before stitching:
     *
     *             Kubernetes                                     Prometheus
     *
     *      ------ service ------                       service1-proxy    service2-proxy
     *     /  \              \   \                            /                 \
     *  app1  app1-sidecar app2  app2-sidecar           app1-proxy          app2-proxy
     *
     * After stitching:
     *
     *          Kubernetes
     *
     *      ------ service ------               <- sold commodities: aggregates used value from Prometheus
     *     /  \             \    \              <- bought commodities: sets used value from Prometheus
     *    /    \             \    \
     *  app1 app1-sidecar  app2  app2-sidecar   <- sold commodities: sets used value from Prometheus
     */
    @Test
    public void testPrometheusOneServiceBuyingFromMultipleApps() throws Exception {
        init(getDataDrivenStitchingOperations());
        final EntityDTO service = EntityBuilders.service("service")
            .selling(CommodityBuilders.application().sold()
                    .key("TwitterApp-demoapp-http://prometheus.istio-system:9090")
                    .used(0d).capacity(100000d))
            .buying(CommodityBuilders.application().
                    from("app1")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934"))
            .buying(CommodityBuilders.application()
                    .from("app1-sidecar")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934-1"))
            .buying(CommodityBuilders.application().
                    from("app2")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934"))
            .buying(CommodityBuilders.application()
                    .from("app2-sidecar")
                    .key("d74685aa-8b98-4e02-a95f-82da8e84c934-1"))
            .property("IP", "service-10.2.1.31,service-10.2.1.32")
            .build();
        final Map<Long, EntityDTO> kubernetesEntities = ImmutableMap.of(
                oid_service, service,
                oid_app1, app1,
                oid_app1_sidecar, app1_sidecar,
                oid_app2, app2,
                oid_app2_sidecar, app2_sidecar
        );
        final Map<Long, EntityDTO> prometheusEntities = ImmutableMap.of(
                oid_service1_proxy, service1_proxy,
                oid_app1_proxy, app1_proxy,
                oid_service2_proxy, service2_proxy,
                oid_app2_proxy, app2_proxy
        );
        addEntities(kubernetesEntities, kubernetesTargetId);
        addEntities(prometheusEntities, prometheusTargetId);

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();

        // verify that there are 9 entities before stitching
        assertEquals(9, stitchingContext.getStitchingGraph().entityCount());

        // stitch
        Map<Long, TopologyEntityDTO.Builder> topology = stitch(stitchingContext);

        // verify that only entities from Kubernetes are left after stitching
        assertEquals(5, topology.size());

        final TopologyEntityDTO.Builder svc = topology.get(oid_service);
        final TopologyEntityDTO.Builder app1 = topology.get(oid_app1);
        final TopologyEntityDTO.Builder app2 = topology.get(oid_app2);

        // verify that service buys commodities from app1 with used value patched from prometheus
        final CommoditiesBoughtFromProvider cb1 = verifyAndGetCommoditiesBought(svc, app1);
        verifyBuyingCommodity(cb1, CommodityType.TRANSACTION_VALUE, "", 12d);
        verifyBuyingCommodity(cb1, CommodityType.RESPONSE_TIME_VALUE, "", 10d);
        verifyBuyingCommodity(cb1, CommodityType.APPLICATION_VALUE,
                "d74685aa-8b98-4e02-a95f-82da8e84c934", 0d);

        // verify that service buys commodities from app2 with used value patched from prometheus
        final CommoditiesBoughtFromProvider cb2 = verifyAndGetCommoditiesBought(svc, app2);
        verifyBuyingCommodity(cb2, CommodityType.TRANSACTION_VALUE, "", 15d);
        verifyBuyingCommodity(cb2, CommodityType.RESPONSE_TIME_VALUE, "", 8d);
        verifyBuyingCommodity(cb2, CommodityType.APPLICATION_VALUE,
                "d74685aa-8b98-4e02-a95f-82da8e84c934", 0d);

        // verify that app1 sells merged sold commodities
        final List<CommoditySoldDTO> app1Sold = verifyAndGetCommoditiesSold(app1);
        verifySoldCommodity(app1Sold, CommodityType.TRANSACTION_VALUE, "", 12d);
        verifySoldCommodity(app1Sold, CommodityType.RESPONSE_TIME_VALUE, "", 10d);
        verifySoldCommodity(app1Sold, CommodityType.APPLICATION_VALUE,
                "d74685aa-8b98-4e02-a95f-82da8e84c934", 0d);

        // verify that app2 sells merged sold commodities
        final List<CommoditySoldDTO> app2Sold = verifyAndGetCommoditiesSold(app2);
        verifySoldCommodity(app2Sold, CommodityType.TRANSACTION_VALUE, "", 15d);
        verifySoldCommodity(app2Sold, CommodityType.RESPONSE_TIME_VALUE, "", 8d);
        verifySoldCommodity(app2Sold, CommodityType.APPLICATION_VALUE,
                "d74685aa-8b98-4e02-a95f-82da8e84c934", 0d);

        // verify that service sells aggregated commodities from all proxy services
        final List<CommoditySoldDTO> svcSold = verifyAndGetCommoditiesSold(svc);
        verifySoldCommodity(svcSold, CommodityType.TRANSACTION_VALUE, "", 27);
        verifySoldCommodity(svcSold, CommodityType.RESPONSE_TIME_VALUE, "", 18);
        verifySoldCommodity(svcSold, CommodityType.APPLICATION_VALUE,
                "TwitterApp-demoapp-http://prometheus.istio-system:9090", 0);
    }

    public void init(List<StitchingOperation<?, ?>> dataDrivenStitchingOperations) {
        setOperationsForProbe(prometheusProbeId, dataDrivenStitchingOperations);
        setOperationsForProbe(kubernetesProbeId, Collections.emptyList());

        stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore,
            targetStore, cpuCapacityStore);
        final Target kubernetesTarget = mock(Target.class);
        when(kubernetesTarget.getId()).thenReturn(kubernetesTargetId);
        final Target prometheusTarget = mock(Target.class);
        when(prometheusTarget.getId()).thenReturn(prometheusTargetId);
        when(targetStore.getProbeTargets(kubernetesProbeId))
            .thenReturn(Collections.singletonList(kubernetesTarget));
        when(targetStore.getProbeTargets(prometheusProbeId))
            .thenReturn(Collections.singletonList(prometheusTarget));
        when(probeStore.getProbe(prometheusProbeId)).thenReturn(Optional.of(ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.CUSTOM.getCategory())
                .setUiProbeCategory(ProbeCategory.CUSTOM.getCategory())
                .setProbeType("Prometheus")
                .build()));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.GUEST_OS_PROCESSES))
            .thenReturn(Collections.singletonList(prometheusProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.CLOUD_NATIVE))
            .thenReturn(Collections.singletonList(kubernetesProbeId));
    }

    private Map<Long, TopologyEntityDTO.Builder> stitch(StitchingContext stitchingContext) {
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
            .configurableStitchingJournalFactory(Clock.systemUTC())
            .addRecorder(new StringBuilderRecorder(new StringBuilder(2048)));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
            .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
            .build());
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
        return stitchingContext.constructTopology();
    }

    private CommoditiesBoughtFromProvider verifyAndGetCommoditiesBought(
            TopologyEntityDTO.Builder consumer, TopologyEntityDTO.Builder provider) {
        List<CommoditiesBoughtFromProvider> bought = consumer.getCommoditiesBoughtFromProvidersList()
            .stream()
            .filter(cb -> cb.getProviderId() == provider.getOid())
            .collect(Collectors.toList());
        assertEquals(1, bought.size());
        return bought.get(0);
    }

    private void verifyBuyingCommodity(CommoditiesBoughtFromProvider cb, int commodityType,
                                       String key, double used) {
        assertEquals(1, cb.getCommodityBoughtList().stream()
            .filter(comm -> comm.getCommodityType().getType() == commodityType)
            .filter(comm -> comm.getCommodityType().getKey().equals(key))
            .filter(comm -> comm.getUsed() == used)
            .count());
    }

    private List<CommoditySoldDTO> verifyAndGetCommoditiesSold(TopologyEntityDTO.Builder entity) {
        final List<CommoditySoldDTO> sold = entity.getCommoditySoldListList();
        assertTrue(sold.size() > 0);
        return sold;
    }

    private void verifySoldCommodity(final List<CommoditySoldDTO> soldCommodities,
                                     int commodityType, final String key, double used) {
        assertEquals(1, soldCommodities.stream()
                .filter(comm -> comm.getCommodityType().getType() == commodityType)
                .filter(comm -> comm.getCommodityType().getKey().equals(key))
                .filter(comm -> comm.getUsed() == used)
                .count());
    }

    private List<StitchingOperation<?, ?>> getDataDrivenStitchingOperations() {
        final MergedEntityMetadata appMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP")
            .mergedSoldCommodity(CommodityType.TRANSACTION)
            .mergedSoldCommodity(CommodityType.RESPONSE_TIME)
            .build();
        // Making sure stitching operation for Service is before that for ApplicationComponent
        return ImmutableList.of(
                new ServiceSLOStitchingOperation(),
                createDataDrivenStitchingOperation(appMergedEntityMetadata,
                        EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE));
    }
}
