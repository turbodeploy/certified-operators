package com.vmturbo.topology.processor.stitching.integration;

import static org.junit.Assert.assertEquals;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
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

    private static final long oid_service1 = 11L;
    private static final long oid_app1 = 21L;
    private static final long oid_app1_1 = 22L;

    private static final long oid_service1_proxy = 111L;
    private static final long oid_app1_proxy = 211L;
    private static final long oid_service1_1_proxy = 112L;
    private static final long oid_app1_1_proxy = 212L;
    private static final long oid_service1_proxy_future = 113L;

    // kubernetes entities
    private final EntityDTO service = EntityBuilders.service("service1")
        .buying(CommodityBuilders.transactionsPerSecond().from("app1").key("10.2.1.31"))
        .buying(CommodityBuilders.responseTimeMillis().from("app1").key("10.2.1.31"))
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-1").key("10.2.1.31-1"))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-1").key("10.2.1.31-1"))
        .property("IP", "service-10.2.1.31,service-10.2.1.31-1")
        .build();

    private final EntityDTO app1 = EntityBuilders.applicationComponent("app1")
        .selling(CommodityBuilders.transactionsPerSecond().sold().key("10.2.1.31"))
        .selling(CommodityBuilders.responseTimeMillis().sold().key("10.2.1.31"))
        .property("IP", "10.2.1.31")
        .build();

    private final EntityDTO app1_1 = EntityBuilders.applicationComponent("app1-1")
        .selling(CommodityBuilders.transactionsPerSecond().sold().key("10.2.1.31-1"))
        .selling(CommodityBuilders.responseTimeMillis().sold().key("10.2.1.31-1"))
        .property("IP", "10.2.1.31-1")
        .build();

    // prometheus proxy entities
    private final EntityDTO service1_proxy = EntityBuilders.service("service1-proxy")
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-proxy").key("10.2.1.31").used(12d))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-proxy").key("10.2.1.31").used(10d))
        .property("IP", "service-10.2.1.31")
        .proxy()
        .build();

    private final EntityDTO app1_proxy = EntityBuilders.applicationComponent("app1-proxy")
        .selling(CommodityBuilders.transactionsPerSecond().sold().key("10.2.1.31").used(12d))
        .selling(CommodityBuilders.responseTimeMillis().sold().key("10.2.1.31").used(10d))
        .property("IP", "10.2.1.31")
        .proxy()
        .build();

    private final EntityDTO service1_1_proxy = EntityBuilders.service("service1-1-proxy")
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-1-proxy").key("10.2.1.31-1").used(15d))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-1-proxy").key("10.2.1.31-1").used(8d))
        .property("IP", "service-10.2.1.31-1")
        .proxy()
        .build();

    private final EntityDTO app1_1_proxy = EntityBuilders.applicationComponent("app1-1-proxy")
        .selling(CommodityBuilders.transactionsPerSecond().sold().key("10.2.1.31-1").used(15d))
        .selling(CommodityBuilders.responseTimeMillis().sold().key("10.2.1.31-1").used(8d))
        .property("IP", "10.2.1.31-1")
        .proxy()
        .build();

    // prometheus proxy entities (new service DTO after potential change in probe in future)
    private final EntityDTO service1_proxy_future = EntityBuilders.service("service1-proxy-future")
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-proxy").key("10.2.1.31").used(12d))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-proxy").key("10.2.1.31").used(10d))
        .buying(CommodityBuilders.transactionsPerSecond().from("app1-1-proxy").key("10.2.1.31-1").used(15d))
        .buying(CommodityBuilders.responseTimeMillis().from("app1-1-proxy").key("10.2.1.31-1").used(8d))
        .property("IP", "service-10.2.1.31,service-10.2.1.31-1")
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
     *     Kubernetes                  Prometheus
     *
     *       service1             service1-proxy    service1-1-proxy
     *       /   \                  |               |
     *    app1  app1-1          app1-proxy     app1-1-proxy
     *
     * After stitching:
     *
     *     Kubernetes
     *
     *       servicep1          (bought commodities got used value from Prometheus)
     *       /   \
     *    app1  app1-1      (sold commodities got used value from Prometheus)
     *
     * @throws Exception if anything goes wrong.
     */
    @Test
    public void testPrometheusOneServiceBuyingFromOneApp() throws Exception {
        init(getDataDrivenStitchingOperations());
        final Map<Long, EntityDTO> kubernetesEntities = ImmutableMap.of(oid_service1, service,
            oid_app1, app1,
            oid_app1_1, app1_1
        );
        final Map<Long, EntityDTO> prometheusEntities = ImmutableMap.of(oid_service1_proxy, service1_proxy,
            oid_app1_proxy, app1_proxy, oid_service1_1_proxy, service1_1_proxy,
            oid_app1_1_proxy, app1_1_proxy
        );
        addEntities(kubernetesEntities, kubernetesTargetId);
        addEntities(prometheusEntities, prometheusTargetId);

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();

        // verify that there are 7 entities before stitching
        assertEquals(7, stitchingContext.getStitchingGraph().entityCount());

        // stitch
        Map<Long, TopologyEntityDTO.Builder> topology = stitch(stitchingContext);

        // verify that only entities from Kubernetes are left after stitching
        assertEquals(3, topology.size());

        final TopologyEntityDTO.Builder service = topology.get(oid_service1);
        final TopologyEntityDTO.Builder app1 = topology.get(oid_app1);
        final TopologyEntityDTO.Builder app11 = topology.get(oid_app1_1);

        // verify that service buys commodities from app1 with used value patched from prometheus
        CommoditiesBoughtFromProvider cb1 = verifyAndGetCommoditiesBought(service, app1);
        assertEquals(2, cb1.getCommodityBoughtCount());
        verifyBuyingCommodity(cb1, CommodityType.TRANSACTION_VALUE, "10.2.1.31", 12d);
        verifyBuyingCommodity(cb1, CommodityType.RESPONSE_TIME_VALUE, "10.2.1.31", 10d);

        // verify that service buys commodities from app1_1 with used value patched from prometheus
        CommoditiesBoughtFromProvider cb11 = verifyAndGetCommoditiesBought(service, app11);
        assertEquals(2, cb11.getCommodityBoughtCount());
        verifyBuyingCommodity(cb11, CommodityType.TRANSACTION_VALUE, "10.2.1.31-1", 15d);
        verifyBuyingCommodity(cb11, CommodityType.RESPONSE_TIME_VALUE, "10.2.1.31-1", 8d);
    }


    /**
     * Test the stitching between Kubernetes and Prometheus. After checking with probe writer,
     * there may be a change in Prometheus to return DTOs in new structure: it returns same
     * structure as Kubernetes, rather than in pairs. This tests the stitching after possible
     * changes in probe.
     *
     * The EntityDTOs used in the test will be the following:
     *
     * Before stitching:
     *
     *     Kubernetes                  Prometheus
     *
     *       service1                  service1-proxy-future
     *       /   \                     /         \
     *    app1  app1-1          app1-proxy    app1-1-proxy
     *
     * After stitching:
     *
     *     Kubernetes
     *
     *       service1          <- bought commodities got used value from Prometheus
     *       /   \
     *    app1  app1-1      <- sold commodities got used value from Prometheus
     */
    @Test
    public void testPrometheusOneServiceBuyingFromMultipleApps() throws Exception {
        init(getDataDrivenStitchingOperationsFuture());
        final Map<Long, EntityDTO> kubernetesEntities = ImmutableMap.of(oid_service1, service,
            oid_app1, app1,
            oid_app1_1, app1_1
        );
        final Map<Long, EntityDTO> prometheusEntities = ImmutableMap.of(oid_service1_proxy_future, service1_proxy_future,
            oid_app1_proxy, app1_proxy,
            oid_app1_1_proxy, app1_1_proxy
        );
        addEntities(kubernetesEntities, kubernetesTargetId);
        addEntities(prometheusEntities, prometheusTargetId);

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();

        // verify that there are 6 entities before stitching
        assertEquals(6, stitchingContext.getStitchingGraph().entityCount());

        // stitch
        Map<Long, TopologyEntityDTO.Builder> topology = stitch(stitchingContext);

        // verify that only entities from Kubernetes are left after stitching
        assertEquals(3, topology.size());

        final TopologyEntityDTO.Builder service = topology.get(oid_service1);
        final TopologyEntityDTO.Builder app1 = topology.get(oid_app1);
        final TopologyEntityDTO.Builder app11 = topology.get(oid_app1_1);

        // verify that service buys commodities from app1 with used value patched from prometheus
        CommoditiesBoughtFromProvider cb1 = verifyAndGetCommoditiesBought(service, app1);
        assertEquals(2, cb1.getCommodityBoughtCount());
        verifyBuyingCommodity(cb1, CommodityType.TRANSACTION_VALUE, "10.2.1.31", 12d);
        verifyBuyingCommodity(cb1, CommodityType.RESPONSE_TIME_VALUE, "10.2.1.31", 10d);

        // verify that service buys commodities from app1_1 with used value patched from prometheus
        CommoditiesBoughtFromProvider cb11 = verifyAndGetCommoditiesBought(service, app11);
        assertEquals(2, cb11.getCommodityBoughtCount());
        verifyBuyingCommodity(cb11, CommodityType.TRANSACTION_VALUE, "10.2.1.31-1", 15d);
        verifyBuyingCommodity(cb11, CommodityType.RESPONSE_TIME_VALUE, "10.2.1.31-1", 8d);
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
        when(probeStore.getProbe(prometheusProbeId)).thenReturn(Optional.empty());
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

    private List<StitchingOperation<?, ?>> getDataDrivenStitchingOperations() {
        final MergedEntityMetadata serviceMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP", ",")
            .mergedBoughtCommodity(EntityType.APPLICATION_COMPONENT, ImmutableList.of(
                CommodityType.TRANSACTION, CommodityType.RESPONSE_TIME))
            .build();

        final MergedEntityMetadata appMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP")
            .mergedSoldCommodity(CommodityType.TRANSACTION)
            .mergedSoldCommodity(CommodityType.RESPONSE_TIME)
            .build();

        return ImmutableList.of(createDataDrivenStitchingOperation(serviceMergedEntityMetadata,
                EntityType.SERVICE, ProbeCategory.CLOUD_NATIVE),
                createDataDrivenStitchingOperation(appMergedEntityMetadata,
                        EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE));
    }

    private List<StitchingOperation<?, ?>> getDataDrivenStitchingOperationsFuture() {
        final MergedEntityMetadata serviceMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP",",")
            .externalMatchingProperty("IP", ",")
            .mergedBoughtCommodity(EntityType.APPLICATION_COMPONENT, ImmutableList.of(
                CommodityType.TRANSACTION, CommodityType.RESPONSE_TIME))
            .build();

        final MergedEntityMetadata appMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP")
            .mergedSoldCommodity(CommodityType.TRANSACTION)
            .mergedSoldCommodity(CommodityType.RESPONSE_TIME)
            .build();

        return ImmutableList.of(createDataDrivenStitchingOperation(serviceMergedEntityMetadata,
                EntityType.SERVICE, ProbeCategory.CLOUD_NATIVE),
                createDataDrivenStitchingOperation(appMergedEntityMetadata,
                        EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE));
    }
}
