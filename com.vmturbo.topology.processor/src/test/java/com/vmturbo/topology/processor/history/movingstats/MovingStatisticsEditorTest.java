package com.vmturbo.topology.processor.history.movingstats;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.ContainerSpecInfoImpl;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.diagnostics.ZipStreamBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CPUThrottlingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.BlobPersistingCachingHistoricalEditorTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCapacityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Tests for {@link MovingStatisticsEditor}.
 */
public class MovingStatisticsEditorTest extends MovingStatisticsBaseTest {

    private static final Clock clock = Mockito.mock(Clock.class);

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private MovingStatisticsEditorCacheAccess movingStatisticsEditor;

    private static final MovingStatisticsHistoricalEditorConfig MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG =
        movingStatisticsHistoricalEditorConfig(clock,
            Collections.singletonList(THROTTLING_SAMPLER_CONFIGURATION));

    private static final SystemNotificationProducer systemNotificationProducer = Mockito.mock(
            SystemNotificationProducer.class);

    private static final long OID1 = 12345L;
    private static final long OID2 = 23456L;
    private static final long OID3 = 34567L;

    private static final CommodityTypeImpl VCPU_COMMODITY_TYPE = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VCPU_VALUE);

    private static final CommodityTypeImpl VCPU_THROTTLING_COMMODITY_TYPE = new CommodityTypeImpl()
        .setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE);

    final TopologyEntityImpl containerSpec1 = new TopologyEntityImpl()
        .setOid(OID1)
        .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
        .setOrigin(new OriginImpl().setDiscoveryOrigin(new DiscoveryOriginImpl()
            .setLastUpdatedTime(System.currentTimeMillis())))
        .addCommoditySoldList(new CommoditySoldImpl()
            .setCommodityType(VCPU_COMMODITY_TYPE)
            .setCapacity(100.0)
            .setUsed(50.0))
        .addCommoditySoldList(new CommoditySoldImpl()
            .setCommodityType(VCPU_THROTTLING_COMMODITY_TYPE)
            .setCapacity(100.0)
            .setUsed(50.0));
    final TopologyEntityImpl containerSpec2 = new TopologyEntityImpl()
        .setOid(OID2)
        .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
        .setOrigin(new OriginImpl().setDiscoveryOrigin(new DiscoveryOriginImpl()
            .setLastUpdatedTime(System.currentTimeMillis())))
        .addCommoditySoldList(new CommoditySoldImpl()
            .setCommodityType(VCPU_COMMODITY_TYPE)
            .setCapacity(200.0)
            .setUsed(75.0))
        .addCommoditySoldList(new CommoditySoldImpl()
            .setCommodityType(VCPU_THROTTLING_COMMODITY_TYPE)
            .setCapacity(100.0)
            .setUsed(20.0));
    final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(
        ImmutableMap.of(
            OID1, TopologyEntity.newBuilder(containerSpec1),
            OID2, TopologyEntity.newBuilder(containerSpec2)));
    final CommodityFieldAccessor fieldAccessor = new CommodityFieldAccessor(graph);
    final GraphWithSettings graphWithSettings = new GraphWithSettings(graph, Collections.emptyMap(), Collections.emptyMap());

    final HistoryAggregationContext realTimeContext = new HistoryAggregationContext(TOPOLOGY_INFO,
        graphWithSettings, false);
    final HistoryAggregationContext planContext = new HistoryAggregationContext(TOPOLOGY_INFO,
        graphWithSettings, true);

    private static final EntityCommodityFieldReference VCPU_REF1 =
        new EntityCommodityFieldReference(OID1, VCPU_COMMODITY_TYPE, null, CommodityField.USED);
    private static final EntityCommodityFieldReference VCPU_REF2 =
        new EntityCommodityFieldReference(OID2, VCPU_COMMODITY_TYPE, null, CommodityField.USED);
    private static final EntityCommodityFieldReference VCPU_REF3 =
        new EntityCommodityFieldReference(OID3, VCPU_COMMODITY_TYPE, null, CommodityField.USED);
    private static final EntityCommodityFieldReference THROTTLING_REF1 =
        new EntityCommodityFieldReference(OID1, VCPU_THROTTLING_COMMODITY_TYPE, null, CommodityField.USED);
    private static final EntityCommodityFieldReference THROTTLING_REF2 =
        new EntityCommodityFieldReference(OID2, VCPU_THROTTLING_COMMODITY_TYPE, null, CommodityField.USED);

    private StatsHistoryServiceMole history;
    private GrpcTestServer grpcServer;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyId(77777L).build();
    private MovingStatisticsStub persistenceTask;

    /**
     * Initializes the tests.
     *
     * @throws IOException if error occurred while creating gRPC server
     */
    @Before
    public void setup() throws IOException {
        history = Mockito.spy(new StatsHistoryServiceMole());
        grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();

        persistenceTask = Mockito.spy(new MovingStatisticsStub(
            StatsHistoryServiceGrpc.newStub(grpcServer.getChannel())));
    }

    /**
     * Cleans up resources.
     */
    @After
    public void shutdown() {
        grpcServer.close();
    }

    /**
     * Set up the test.
     *
     * @throws IOException when failed
     * @throws IdentityUninitializedException if identity provider is not initialized.
     */
    @Before
    public void setUp() throws IOException, IdentityUninitializedException {
        final LongSet oidsInIdentityCache = new LongOpenHashSet();

        movingStatisticsEditor =
            new MovingStatisticsEditorCacheAccess(MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG, null,
                clock, this::createPersistenceTask, systemNotificationProducer, identityProvider);
        Mockito.when(identityProvider.getCurrentOidsInIdentityCache()).thenReturn(oidsInIdentityCache);
    }

    /**
     * Checks that exporting and loading of moving statistics diagnostics is working as expected.
     *
     * @throws DiagnosticsException in case of error during diagnostics
     *                 export/import
     * @throws InterruptedException in case test has been interrupted.
     * @throws HistoryCalculationException in case context initialization failed.
     * @throws IOException diagnostics collection could not write state into stream.
     */
    @Test
    public void checkRestoreDiags()
        throws DiagnosticsException, InterruptedException, HistoryCalculationException,
        IOException {
        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());

        final Map<Pair<EntityCommodityFieldReference, EntityCommodityFieldReference>, Double> originalMeans =
            Stream.of(Pair.create(VCPU_REF1, THROTTLING_REF1), Pair.create(VCPU_REF2, THROTTLING_REF2))
                .collect(Collectors.toMap(p -> p,
                    p -> movingStatisticsEditor.getCache().get(p.getFirst())
                        .getSampler()
                        .meanPlusSigma(p.getSecond(), 0)));

        BlobPersistingCachingHistoricalEditorTest.createKvConfig(MovingStatisticsHistoricalEditorConfig.ADD_TO_DIAGNOSTICS_PROPERTY);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            movingStatisticsEditor.collectDiags(output);
            movingStatisticsEditor.restoreDiags(output.toByteArray(), null);
        }

        originalMeans.keySet().forEach(referencePair -> {
            assertEquals(originalMeans.get(referencePair),
                movingStatisticsEditor.getCache().get(referencePair.getFirst())
                .getSampler()
                .meanPlusSigma(referencePair.getSecond(), 0));
        });
    }

    /**
     * Checks that binary diags output contains the correct file and contents.
     *
     * @throws InterruptedException in case test has been interrupted
     * @throws HistoryCalculationException in case context initialization failed
     * @throws IOException in case of failure to write bits
     * @throws DiagnosticsException in case of error generating diags
     */
    @Test
    public void testExportOutput() throws HistoryCalculationException, InterruptedException,
        IOException, DiagnosticsException {
        // First initialize some moving statistics data
        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());
        BlobPersistingCachingHistoricalEditorTest.createKvConfig(MovingStatisticsHistoricalEditorConfig.ADD_TO_DIAGNOSTICS_PROPERTY);

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            movingStatisticsEditor.collectDiags(output);
            final Iterable<Diags> diagsReader = new DiagsZipReader(
                new ByteArrayInputStream(output.toByteArray()), null, true);
            final Iterator<Diags> iter = diagsReader.iterator();
            // Checking here that exactly 1 entry exists and is parseable
            Assert.assertTrue(iter.hasNext());
            checkDiags(iter.next());
            Assert.assertFalse(iter.hasNext());
        }
    }

    /**
     * Test the generic applicability of moving statistics calculation.
     */
    @Test
    public void testIsApplicable() {
        Assert.assertTrue(movingStatisticsEditor.isApplicable(Collections.emptyList(),
            TopologyInfo.newBuilder().build(), null));

        // moving statistics should be set for custom plan project is created by the user
        Assert.assertTrue(movingStatisticsEditor.isApplicable(Collections.emptyList(),
            TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.USER))
                .build(), null));

        // moving statistics should not be set for cluster headroom plans
        Assert.assertFalse(movingStatisticsEditor.isApplicable(Collections.emptyList(),
            TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                .build(), null));

        Assert.assertTrue(movingStatisticsEditor.isApplicable(
            Collections.singletonList(ScenarioChange.newBuilder().build()),
            TopologyInfo.newBuilder().build(), null));

        // moving statistics should not be set for baseline
        Assert.assertFalse(movingStatisticsEditor.isApplicable(Collections.singletonList(
            ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                    .setHistoricalBaseline(
                        HistoricalBaseline.newBuilder().setBaselineDate(0).build()))
                .build()), TopologyInfo.newBuilder().build(), null));
    }

    /**
     * Test the applicability of moving statistics calculation at the entity level.
     */
    @Test
    public void testIsEntityApplicable() {
        // Moving statistics not applicable for container entities
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(1L)
                .setEntityType(EntityType.CONTAINER_VALUE)).build()));

        // Moving statistics not applicable for container pod entities
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(2L)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)).build()));

        // Moving statistics not applicable for container pod entities
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(3L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)).build()));

        // Moving statistics not applicable for container pod entities
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setOid(4L)
                .setEntityType(EntityType.BUSINESS_USER_VALUE)).build()));

        // Moving statistics not applicable for entity with not discovered origin from targets
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setOrigin(new OriginImpl()
                    .setDiscoveryOrigin(new DiscoveryOriginImpl()
                        .putDiscoveredTargetData(1L,
                            new PerTargetEntityInformationImpl()
                                .setOrigin(EntityOrigin.PROXY))))).build()));

        // Moving statistics not applicable for entity without time-based CPU throttling metrics
        Assert.assertFalse(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setOrigin(new OriginImpl()
                    .setDiscoveryOrigin((new DiscoveryOriginImpl()
                        .putDiscoveredTargetData(1L,
                            new PerTargetEntityInformationImpl()
                                .setOrigin(EntityOrigin.DISCOVERED)))))
        ).build()));

        // moving statistics should still be set for controllable false entity if it is ContainerSpec
        Assert.assertTrue(movingStatisticsEditor.isEntityApplicable(TopologyEntity.newBuilder(
            new TopologyEntityImpl()
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setOrigin(new OriginImpl()
                    .setDiscoveryOrigin((new DiscoveryOriginImpl()
                        .putDiscoveredTargetData(1L,
                            new PerTargetEntityInformationImpl()
                                .setOrigin(EntityOrigin.DISCOVERED)))))
                .setAnalysisSettings(new AnalysisSettingsImpl().setControllable(false))
                .setTypeSpecificInfo(new TypeSpecificInfoImpl()
                    .setContainerSpec(new ContainerSpecInfoImpl()
                        .setCpuThrottlingType(CPUThrottlingType.timeBased))))
            .build()));
    }

    /**
     * Test the applicability of moving statistics calculation at the commodity level.
     */
    @Test
    public void testIsCommodityApplicable() {
        // Don't set moving statistics for commodity without sampling configuration
        Assert.assertFalse(movingStatisticsEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(new TopologyEntityImpl()).build(),
            new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityDTO.CommodityType.VMEM_VALUE)
            ), topologyInfo));
        // Set moving statistics for commodity with sampling configuration
        Assert.assertTrue(movingStatisticsEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(new TopologyEntityImpl()).build(),
            new CommoditySoldImpl().setCommodityType(
                new CommodityTypeImpl().setType(CommodityDTO.CommodityType.VCPU_VALUE)
            ), topologyInfo));
    }

    /**
     * Test that we save data via the persistence task on a realtime broadcast.
     *
     * @throws HistoryCalculationException on exception
     * @throws InterruptedException on exception
     */
    @Test
    public void testSaveOnCompleteBroadcast() throws HistoryCalculationException, InterruptedException {
        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());

        movingStatisticsEditor.completeBroadcast(realTimeContext);
        Mockito.verify(persistenceTask).save(any(MovingStatistics.class), anyLong(),
            eq(MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG));
    }

    /**
     * Test that we do not save data via the persistence task on a plan broadcast.
     *
     * @throws HistoryCalculationException on exception
     * @throws InterruptedException on exception
     */
    @Test
    public void testNoSaveOnPlanCompleteBroadcast() throws HistoryCalculationException, InterruptedException {
        movingStatisticsEditor.initContext(planContext, Collections.emptyList());

        movingStatisticsEditor.completeBroadcast(planContext);
        Mockito.verify(persistenceTask, Mockito.never()).save(any(MovingStatistics.class), anyLong(),
            eq(MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG));
    }

    /**
     * Test that data is only loaded from the persistent store once even when initContext is
     * called multiple times.
     *
     * @throws HistoryCalculationException on exception
     * @throws InterruptedException on exception
     */
    @Test
    public void testDataLoadedOnlyOnce() throws HistoryCalculationException, InterruptedException {
        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());
        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());
        Mockito.verify(persistenceTask, times(1))
            .load(anyCollection(), eq(MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG), any(LongSet.class));
    }

    /**
     * Test that data loaded to the persistent store is additive to the references directly
     * passed to initContext.
     *
     * @throws HistoryCalculationException on exception
     * @throws InterruptedException on exception
     */
    @Test
    public void testLoadedDataIsAdditive() throws HistoryCalculationException, InterruptedException {
        movingStatisticsEditor.initContext(realTimeContext, Collections.singletonList(VCPU_REF3));

        // One record should have been retained from directly passing to initContext, and two
        // records should have been loaded from the persistent store.
        assertEquals(3, movingStatisticsEditor.getCache().size());
        assertThat(movingStatisticsEditor.getCache().keySet(),
            containsInAnyOrder(VCPU_REF1, VCPU_REF2, VCPU_REF3));
    }

    /**
     * Verify that we do not load persisted data on initContext after restoring diagnostics.
     *
     * @throws HistoryCalculationException on exception
     * @throws InterruptedException on exception
     * @throws IOException on exception
     * @throws DiagnosticsException on exception
     */
    @Test
    public void testDataIsNotLoadedAfterDiagnosticsRestore() throws HistoryCalculationException,
        InterruptedException, IOException, DiagnosticsException {
        final MovingStatistics build = MovingStatistics.newBuilder().addStatisticRecords(movingStatisticsRecord(VCPU_REF3)).build();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        build.writeDelimitedTo(baos);

        final ZipStreamBuilder builder = ZipStreamBuilder.builder()
            .withFile(movingStatisticsEditor.getFileName() + DiagsZipReader.BINARY_DIAGS_SUFFIX,
                baos.toByteArray());
        movingStatisticsEditor.restoreDiags(builder.getBytes(), null);


        movingStatisticsEditor.initContext(realTimeContext, Collections.emptyList());
        verify(persistenceTask, never()).load(anyCollection(), eq(MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG), any(LongSet.class));
        assertEquals(1, movingStatisticsEditor.getCache().size());
        assertThat(movingStatisticsEditor.getCache().keySet(), contains(VCPU_REF3));
    }

    private MovingStatisticsStub createPersistenceTask(StatsHistoryServiceStub stub, Pair<Long, Long> range) {
        return persistenceTask;
    }

    private static void checkDiags(@Nonnull final Diags diags) throws IOException {
        Assert.assertNotNull(diags.getBytes());
        try (InputStream is = new ByteArrayInputStream(diags.getBytes())) {
            final MovingStatistics movingStatistics = MovingStatistics.parseDelimitedFrom(is);
            assertEquals(2, movingStatistics.getStatisticRecordsCount());
        }
    }

    /**
     * Access to the moving statistics editor cached data.
     */
    private static class MovingStatisticsEditorCacheAccess extends MovingStatisticsEditor {
        MovingStatisticsEditorCacheAccess(MovingStatisticsHistoricalEditorConfig config,
                                    StatsHistoryServiceStub statsHistoryClient,
                                    Clock clock,
                                    BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, MovingStatisticsPersistenceTask> taskCreator,
                                    @Nonnull SystemNotificationProducer systemNotificationProducer,
                                    IdentityProvider identityProvider) {
            super(config, statsHistoryClient, taskCreator, clock, systemNotificationProducer, identityProvider);
        }

        MovingStatisticsCommodityData getCacheEntry(EntityCommodityFieldReference field) {
            return getCache().get(field);
        }

        @Override
        public Map<EntityCommodityFieldReference, MovingStatisticsCommodityData> getCache() {
            return super.getCache();
        }
    }

    private static MovingStatisticsRecord movingStatisticsRecord(@Nonnull  EntityCommodityFieldReference ref) {
        return MovingStatisticsRecord.newBuilder()
            .setEntityOid(ref.getEntityOid())
            .setThrottlingRecord(ThrottlingMovingStatisticsRecord.newBuilder()
                .setActiveVcpuCapacity(10.0)
                .addCapacityRecords(ThrottlingCapacityMovingStatistics.newBuilder()
                    .setLastSampleTimestamp(100L)
                    .setSampleCount(50)
                    .setThrottlingFastMovingAverage(20.0)
                    .setThrottlingSlowMovingAverage(30.0)
                    .setVcpuCapacity(10.0)
                    .build())
                .build())
            .build();
    }

    /**
     * Helper for load/save operations without use to GRPC.
     */
    private static class MovingStatisticsStub extends MovingStatisticsPersistenceTask {

        private Map<EntityCommodityFieldReference, MovingStatisticsRecord> resultForLoad;
        private List<MovingStatisticsCommodityData> commodityData = new ArrayList<>();

        private MovingStatisticsStub(@Nullable StatsHistoryServiceStub unused) {
            super(unused, clock, Pair.create(0L, 0L), false, MOVING_STATISTICS_HISTORICAL_EDITOR_CONFIG);

            resultForLoad = new HashMap<>();
            Stream.of(VCPU_REF1, VCPU_REF2).forEach(ref -> {
                final MovingStatisticsCommodityData data = Mockito.spy(new MovingStatisticsCommodityData());
                resultForLoad.put(ref, movingStatisticsRecord(ref));
            });
        }

        private List<MovingStatisticsCommodityData> getCommodityData() {
            return commodityData;
        }

        @Override
        public Map<EntityCommodityFieldReference, MovingStatisticsRecord> load(
            @Nonnull Collection<EntityCommodityReference> commodities,
            @Nonnull MovingStatisticsHistoricalEditorConfig config,
            @Nonnull final Set<Long> oidsToUse) {
            // Current implementation doesn't use this parameter.
            // If this assertion fails then most likely the implementation changed and
            // you should add more unit tests for that.
            Assert.assertTrue(commodities.isEmpty());
            return Collections.unmodifiableMap(resultForLoad);
        }

        @Override
        public void save(@Nonnull MovingStatistics stats,
                         long periodMs,
                         @Nonnull MovingStatisticsHistoricalEditorConfig config) {
            // UNUSED
        }
    }
}
