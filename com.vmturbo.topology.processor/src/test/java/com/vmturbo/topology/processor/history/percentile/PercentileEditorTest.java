package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.NotificationDTO;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor.CacheBackup;
import com.vmturbo.topology.processor.history.BlobPersistingCachingHistoricalEditorTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord.CapacityChange;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit test for {@link PercentileEditor}.
 */
public class PercentileEditorTest extends PercentileBaseTest {
    private static final long TIMESTAMP_AUG_28_2019_12_00 = 1566993600000L;
    private static final long TIMESTAMP_AUG_29_2019_00_00 = 1567036800000L;
    private static final long TIMESTAMP_AUG_29_2019_06_00 = 1567058400000L;
    private static final long TIMESTAMP_AUG_29_2019_12_00 = 1567080000000L;
    private static final long TIMESTAMP_AUG_30_2019_00_00 = 1567123200000L;
    private static final long TIMESTAMP_AUG_30_2019_12_00 = 1567166400000L;
    private static final long TIMESTAMP_AUG_31_2019_00_00 = 1567209600000L;
    private static final long TIMESTAMP_AUG_31_2019_12_00 = 1567252800000L;
    private static final long TIMESTAMP_INIT_START_SEP_1_2019 = 1567296000000L;
    private static final long TIMESTAMP_SEP_1_2019_12_00 = 1567339200000L;
    private static final long TIMESTAMP_SEP_2_2019_00_00 = 1567382400000L;
    private static final long TIMESTAMP_SEP_5_2019 = 1567641600000l;

    private static final int MAINTENANCE_WINDOW_HOURS = 12;
    private static final long MAINTENANCE_WINDOW_MS = TimeUnit.HOURS.toMillis(MAINTENANCE_WINDOW_HOURS);
    private static final String PERCENTILE_BUCKETS_SPEC = "0,1,5,99,100";
    private static final KVConfig KV_CONFIG = BlobPersistingCachingHistoricalEditorTest.createKvConfig(
        PercentileHistoricalEditorConfig.STORE_CACHE_TO_DIAGNOSTICS_PROPERTY);
    private static final PercentileHistoricalEditorConfig PERCENTILE_HISTORICAL_EDITOR_CONFIG =
                    new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 777777L, 10,
                                    100,
                                    ImmutableMap.of(CommodityType.VCPU, PERCENTILE_BUCKETS_SPEC,
                                                    CommodityType.IMAGE_CPU,
                                                    PERCENTILE_BUCKETS_SPEC), KV_CONFIG,
                                    Clock.systemUTC());
    private static final SystemNotificationProducer systemNotificationProducer = Mockito.mock(
            SystemNotificationProducer.class);

    private static final long VIRTUAL_MACHINE_OID = 1;
    private static final long VIRTUAL_MACHINE_OID_2 = 2;
    private static final long BUSINESS_USER_OID = 10;
    private static final long BUSINESS_USER_OID_2 = 11;
    private static final long DATABASE_SERVER_OID = 100;
    private static final long CONTAINER_OID = 1000;
    private static final long CONTAINER_POD_OID = 2000;
    private static final long DESKTOP_POOL_PROVIDER_OID = 3;
    private static final long PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 2;
    private static final long NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD = 4;
    private static final long PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD = 4;
    private static final long NEW_BUSINESS_USER_OBSERVATION_PERIOD = 2;
    private static final float CAPACITY = 1000F;
    private static final String DESKTOP_POOL_COMMODITY_KEY = "DesktopPool::key";
    private static final EntityCommodityFieldReference VCPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(VIRTUAL_MACHINE_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .build(), CommodityField.USED);
    private static final EntityCommodityFieldReference IMAGE_CPU_COMMODITY_REFERENCE =
            new EntityCommodityFieldReference(BUSINESS_USER_OID,
                    TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.VCPU_VALUE)
                            .setKey(DESKTOP_POOL_COMMODITY_KEY)
                            .build(), DESKTOP_POOL_PROVIDER_OID, CommodityField.USED);

    private final Clock clock = Mockito.mock(Clock.class);
    private Map<Long, Builder> topologyBuilderMap;
    private Map<Long, EntitySettings> entitySettings;
    private GraphWithSettings graphWithSettings;
    private PercentileEditorCacheAccess percentileEditor;
    private List<PercentilePersistenceTask> percentilePersistenceTasks;
    private TopologyInfo topologyInfo;
    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    /**
     * Set up the test.
     *
     * @throws IOException when failed
     */
    @Before
    public void setUp() throws IOException, IdentityUninitializedException {
        history = Mockito.spy(new StatsHistoryServiceMole());
        grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();

        setUpTopology();
        LongSet oidsInIdentityCache = new LongOpenHashSet();
        oidsInIdentityCache.addAll(Arrays.asList(VIRTUAL_MACHINE_OID, VIRTUAL_MACHINE_OID_2,
            BUSINESS_USER_OID, BUSINESS_USER_OID_2,
            DATABASE_SERVER_OID, CONTAINER_OID, CONTAINER_POD_OID, DESKTOP_POOL_PROVIDER_OID));
        percentilePersistenceTasks = new ArrayList<>();
        final StatsHistoryServiceStub mockStatsHistoryClient = StatsHistoryServiceGrpc.newStub(grpcServer.getChannel());
        percentileEditor =
                        new PercentileEditorCacheAccess(PERCENTILE_HISTORICAL_EDITOR_CONFIG, mockStatsHistoryClient,
                                        setUpBlockingStub(MAINTENANCE_WINDOW_HOURS),
                                        clock, (service, range) -> {
                            final PercentileTaskStub result =
                                            Mockito.spy(new PercentileTaskStub(service, clock, range));
                            percentilePersistenceTasks.add(result);
                            return result;
                        }, identityProvider);
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(77777L).build();
        Mockito.when(identityProvider.getCurrentOidsInIdentityCache()).thenReturn(oidsInIdentityCache);
    }

    /**
     * Cleans up resources.
     */
    @After
    public void shutdown() {
        super.shutdown();
    }

    private void setUpTopology() {
        topologyBuilderMap = new HashMap<>();
        entitySettings = new HashMap<>();
        addEntityWithSetting(VIRTUAL_MACHINE_OID,
                             EntityType.VIRTUAL_MACHINE_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodVirtualMachine,
                             PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                             topologyBuilderMap, entitySettings);
        addEntityWithSetting(BUSINESS_USER_OID,
                             EntityType.BUSINESS_USER_VALUE,
                             EntitySettingSpecs.MaxObservationPeriodBusinessUser,
                             PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD,
                             topologyBuilderMap, entitySettings);
        graphWithSettings = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                                                                  .newGraph(topologyBuilderMap),
                                                  entitySettings,
                                                  Collections.emptyMap());
    }

    /**
     * Test the generic applicability of percentile calculation.
     */
    @Test
    public void testIsApplicable() {
        Assert.assertTrue(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder().build(), null));

        // Percentile should be set for custom plan project is created by the user
        Assert.assertTrue(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder()
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanProjectType(PlanProjectType.USER))
                        .build(), null));

        // Percentile should not be set for cluster headroom plans
        Assert.assertFalse(percentileEditor.isApplicable(Collections.emptyList(),
                TopologyInfo.newBuilder()
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                        .build(), null));

        Assert.assertTrue(percentileEditor.isApplicable(
                Collections.singletonList(ScenarioChange.newBuilder().build()),
                TopologyInfo.newBuilder().build(), null));

        // Percentile should not be set for baseline
        Assert.assertFalse(percentileEditor.isApplicable(Collections.singletonList(
                ScenarioChange.newBuilder()
                        .setPlanChanges(PlanChanges.newBuilder()
                                .setHistoricalBaseline(
                                        HistoricalBaseline.newBuilder().setBaselineDate(0).build()))
                        .build()), TopologyInfo.newBuilder().build(), null));
    }

    /**
     * Test the applicability of percentile calculation at the entity level.
     */
    @Test
    public void testIsEntityApplicable() {
        // Percentile should not be set for container entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(CONTAINER_OID)
                .setEntityType(EntityType.CONTAINER_VALUE)).build()));

        // Percentile should not be set for container pod entities
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(CONTAINER_POD_OID)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)).build()));

        // Percentile should be set for virtual machine entities
        Assert.assertTrue(percentileEditor.isEntityApplicable(
                topologyBuilderMap.get(VIRTUAL_MACHINE_OID).build()));

        // Percentile should be set for business user entities
        Assert.assertTrue(percentileEditor.isEntityApplicable(
            topologyBuilderMap.get(BUSINESS_USER_OID).build()));

        // Percentile should not be set for entity with not discovered origin from targets
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(1L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setOrigin(EntityOrigin.PROXY)
                                                        .build())
                                        .build())
                                .build())).build()));

        // Percentile should not be set for entity with controllable false
        Assert.assertFalse(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                    .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(1L,
                                PerTargetEntityInformation.newBuilder()
                                    .setOrigin(EntityOrigin.DISCOVERED)
                                    .build())))
                    .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false).build()))
                .build()));

        // Percentile should still be set for controllable false entity if it is ContainerSpec
        Assert.assertTrue(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .putDiscoveredTargetData(1L,
                            PerTargetEntityInformation.newBuilder()
                                .setOrigin(EntityOrigin.DISCOVERED)
                                .build())))
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false).build()))
            .build()));

        // Percentile should be be set for database server entities.
        Assert.assertTrue(percentileEditor.isEntityApplicable(TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOid(DATABASE_SERVER_OID)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder().setControllable(true))
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(1L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setOrigin(EntityOrigin.DISCOVERED)
                                                        .build())))
                        .setEntityType(EntityType.DATABASE_SERVER_VALUE)).build()));
    }

    /**
     * Test the applicability of percentile calculation at the commodity level.
     */
    @Test
    public void testIsCommodityApplicable() {
        // Don't set percentile for commodity sold without utilization data or required type
        Assert.assertFalse(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommoditySoldDTO.newBuilder(), topologyInfo));
        // Set percentile for commodity sold with utilization data and without required type
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance()), topologyInfo));
        // Set percentile for commodity sold without utilization data and with required type
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VCPU_VALUE)), topologyInfo));
        // Set percentile for commodity sold with utilization data and required type
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance())
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VCPU_VALUE)), topologyInfo));
        // Set percentile for Storage Access commodity sold in MCP.
        Assert.assertTrue(percentileEditor.isCommodityApplicable(TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)).build(),
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                .setType(CommodityType.STORAGE_ACCESS_VALUE)),
                TopologyInfo.newBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()
                        .setPlanType(PlanProjectType.CLOUD_MIGRATION.name())).build()));

        // Don't set percentile for commodity bought without utilization data
        Assert.assertFalse(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder(),
            0));
        // Don't set percentile for commodity bought without enabled type of commodity or provider
        Assert.assertFalse(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance()),
            0));
        // Set percentile for commodity bought with enabled type
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance())
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.IMAGE_CPU_VALUE)),
            0));
        // Don't set percentile for commodity bought from enabled provider type without associated commodity type
        Assert.assertFalse(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance())
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.BALLOONING_VALUE)),
            EntityType.COMPUTE_TIER_VALUE));
        // Set percentile for commodity bought from enabled provider type with enabled commodity type
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance())
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.STORAGE_ACCESS_VALUE)),
            EntityType.COMPUTE_TIER_VALUE));
        // Set percentile for IO Throughput commodity bought from Compute Tier, if
        // utilizationData is present
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()).build(),
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setUtilizationData(UtilizationData.getDefaultInstance())
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.IO_THROUGHPUT_VALUE)),
            EntityType.COMPUTE_TIER_VALUE));
    }

    /**
     * Test that Cloud VMem commodity sold without utilizationData is not processed for
     * percentile calculation.
     */
    @Test
    public void testIsSoldCloudVMemCommodityApplicableNoUtilizationData() {
        Assert.assertFalse(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD))
                .build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.VMEM_VALUE)), topologyInfo));
    }

    /**
     * Test that Cloud VMem commodity sold with utilizationData is processed for percentile
     * calculation.
     */
    @Test
    public void testIsSoldCloudVMemCommodityApplicableWithUtilizationData() {
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setEnvironmentType(EnvironmentType.CLOUD))
                .build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VMEM_VALUE))
                .setUtilizationData(UtilizationData.getDefaultInstance()), topologyInfo));
    }

    /**
     * Test that non-Cloud VMem commodity with no utilizationData is processed for percentile
     * calculation.
     */
    @Test
    public void testIsSoldNonCloudVMemCommodityApplicableNoUtilizationData() {
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder())
                .build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VMEM_VALUE)), topologyInfo));
    }

    /**
     * Test that non-Cloud VMem commodity with utilizationData is processed for percentile
     * calculation.
     */
    @Test
    public void testIsSoldNonCloudVMemCommodityApplicableWithUtilizationData() {
        Assert.assertTrue(percentileEditor.isCommodityApplicable(
            TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder())
                .build(),
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VMEM_VALUE))
                .setUtilizationData(UtilizationData.getDefaultInstance()), topologyInfo));
    }

    /**
     * Test the initial data loading.
     * That requests to load full and latest window blobs are made and results are accumulated.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testLoadData() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db.
        percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                        false), Collections.emptyList());
        // LATEST(1, 2, 3, 4, 5) + TOTAL(41, 42, 43, 44, 45) = FULL(42, 44, 46, 48, 50)
        Assert.assertEquals(Arrays.asList(42, 44, 46, 48, 50),
                percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet(), true)
                        .build()
                        .getUtilizationList());
        // LATEST(46, 47, 48, 49, 50) + TOTAL(86, 87, 88, 89, 90) = FULL(132, 134, 136, 138, 140)
        Assert.assertEquals(Arrays.asList(132, 134, 136, 138, 140),
                percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                        .getUtilizationCountStore()
                        .checkpoint(Collections.emptySet(), true)
                        .build()
                        .getUtilizationList());
    }

    /**
     * Test that {@link PercentileEditor} correctly handles case when observation period of entity
     * changed. Particularly checks that in case observation window changed before next maintenance
     * operation will happen than oldest day will not be subtracted from total utilization points,
     * i.e. total blob from cache will be sent to database without any changes.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChangedSaveTotal()
                    throws InterruptedException, HistoryCalculationException {
        final List<Integer> buUtilizations = Arrays.asList(280, 285, 290, 295, 300);
        final List<Integer> vmUtilizations = Arrays.asList(189, 198, 207, 216, 225);
        checkObservationWindowChanged(TIMESTAMP_INIT_START_SEP_1_2019 + 2000,
                                      TIMESTAMP_INIT_START_SEP_1_2019,
                                      Arrays.asList(vmUtilizations, buUtilizations), buUtilizations,
                                      vmUtilizations,
                                      false, 0, 1);
    }

    /**
     * Test that {@link PercentileEditor} correctly handles case when observation period of entity
     * changed. Particularly checks that in case observation window changed after timestamp when next maintenance
     * operation was scheduled than oldest day will be subtracted from total utilization points,
     * i.e. 'normal' maintenance workflow will be passed.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckObservationPeriodsChangedSaveTotalWithRemoval()
            throws InterruptedException, HistoryCalculationException {
        final long nextScheduledCheckpointMs =
                TIMESTAMP_INIT_START_SEP_1_2019 + TimeUnit.HOURS.toMillis(
                        PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours());
        checkObservationWindowChanged(nextScheduledCheckpointMs + 2000, nextScheduledCheckpointMs,
                                      Arrays.asList(Arrays.asList(189, 198, 207, 216, 225),
                                      Arrays.asList(280, 285, 290, 295, 300)),
                                      Arrays.asList(280, 285, 290, 295, 300),
                                      Arrays.asList(189, 198, 207, 216, 225),
                                      false, 0, 1);
    }

    private void checkObservationWindowChanged(long broadcastTimeAfterWindowChanged,
                                               long periodMsForTotalBlob,
                                               List<List<Integer>> expectedTotalUtilizations,
                                               List<Integer> buUtilizations,
                                               List<Integer> vmUtilizations,
                                               boolean enforceMaintenanceIsExpected,
                                               int dayPersistenceIdx,
                                               int totalPersistenceIdx)
                    throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        final HistoryAggregationContext firstContext =
                        new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        // First initializing history from db.
        percentileEditor.initContext(firstContext, Collections.emptyList());

        // Change observation periods.
        entitySettings.put(VIRTUAL_MACHINE_OID,
                createEntitySetting(VIRTUAL_MACHINE_OID,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                        EntitySettingSpecs.MaxObservationPeriodVirtualMachine));
        entitySettings.put(BUSINESS_USER_OID,
                createEntitySetting(BUSINESS_USER_OID,
                        NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                        EntitySettingSpecs.MaxObservationPeriodBusinessUser));

        // Check observation periods changed.
        final HistoryAggregationContext secondContext =
                        new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        percentileEditor.initContext(secondContext,
                        Collections.emptyList());
        // Check full utilization count array for virtual machine VCPU commodity.
        // 41 42 43 44 45 [x] 28 Aug 2019 12:00:00 GMT.
        // 36 37 38 39 40 [x] 28 Aug 2019 12:00:00 GMT.
        // 31 32 33 34 35 [x] 29 Aug 2019 00:00:00 GMT.
        // 26 27 28 29 30 [x] 29 Aug 2019 12:00:00 GMT.
        // 21 22 23 24 25 [x] 30 Aug 2019 00:00:00 GMT.
        // 16 14 18 19 20 [x] 30 Aug 2019 12:00:00 GMT.
        // 11 12 13 14 15 [x] 31 Aug 2019 00:00:00 GMT.
        //  6  7  8  9 10 [x] 31 Aug 2019 12:00:00 GMT.
        //  1  2  3  4  5 [x]  1 Sep 2019 00:00:00 GMT.
        // -----------------------------------------
        // 189, 198, 207, 216, 225 - full for 4 days of observation.
        checkEntityUtilization(vmUtilizations, VCPU_COMMODITY_REFERENCE,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD);
        // Check full utilization count array for business user ImageCPU commodity.
        // 86 87 88 89 90 [ ] 28 Aug 2019 00:00:00 GMT.
        // 81 82 83 84 85 [ ] 28 Aug 2019 12:00:00 GMT.
        // 76 77 78 79 80 [ ] 29 Aug 2019 00:00:00 GMT.
        // 71 72 73 74 75 [ ] 29 Aug 2019 12:00:00 GMT.
        // 66 67 68 69 70 [x] 30 Aug 2019 00:00:00 GMT.
        // 61 62 63 64 65 [x] 30 Aug 2019 12:00:00 GMT.
        // 56 57 58 59 60 [x] 31 Aug 2019 00:00:00 GMT.
        // 51 52 53 54 55 [x] 31 Aug 2019 12:00:00 GMT.
        // 46 47 48 49 50 [x]  1 Sep 2019 00:00:00 GMT.
        // -----------------------------------------
        // 214 218 222 226 230 - full for 2 days of observation.
        checkEntityUtilization(buUtilizations, IMAGE_CPU_COMMODITY_REFERENCE,
                        NEW_BUSINESS_USER_OBSERVATION_PERIOD);
        // Check that maintenance will be called
        Mockito.when(clock.millis()).thenReturn(broadcastTimeAfterWindowChanged);
        @SuppressWarnings("unchecked")
        final CacheBackup<PercentileCommodityData> cacheBackup = Mockito.mock(CacheBackup.class);
        final PercentileEditorCacheAccess spiedEditor = Mockito.spy(percentileEditor);
        Mockito.when(spiedEditor.createCacheBackup()).thenReturn(cacheBackup);
        percentilePersistenceTasks.clear();
        spiedEditor.completeBroadcast(secondContext);
        checkMaintenance(periodMsForTotalBlob, expectedTotalUtilizations,
            enforceMaintenanceIsExpected, dayPersistenceIdx, totalPersistenceIdx,
            broadcastTimeAfterWindowChanged);
        Mockito.verify(cacheBackup, Mockito.times(1)).keepCacheOnClose();
    }

    /**
     * Test that full blob reassembly happens upon observation periods change.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException never
     * @throws IOException never
     */
    @Test
    public void testReassemblyFullPage() throws InterruptedException, HistoryCalculationException, IOException {
        final PercentileHistoricalEditorConfig config =
                new PercentileHistoricalEditorConfig(1, PercentileHistoricalEditorConfig.DEFAULT_MAINTENANCE_WINDOW_HOURS, 777777L, 10,
                        100,
                        ImmutableMap.of(CommodityType.VCPU, PERCENTILE_BUCKETS_SPEC,
                                CommodityType.IMAGE_CPU,
                                PERCENTILE_BUCKETS_SPEC), KV_CONFIG,
                        Clock.systemUTC());
        final long firstCheckpoint = TIMESTAMP_AUG_28_2019_12_00;
        final StatsHistoryServiceStub mockStatsHistoryClient = StatsHistoryServiceGrpc.newStub(grpcServer.getChannel());
        percentileEditor = new PercentileEditorCacheAccess(config, mockStatsHistoryClient,
                    setUpBlockingStub(MAINTENANCE_WINDOW_HOURS),
                    clock, (service, range) -> {
            final PercentileTaskStub result =
                    Mockito.spy(new PercentileTaskStub(service, clock, range));
            if (percentilePersistenceTasks.isEmpty() && range.getFirst() == 0) {
                Mockito.when(result.getLastCheckpointMs()).thenReturn(firstCheckpoint);
            }
            percentilePersistenceTasks.add(result);
            return result;
        }, identityProvider);
        final HistoryAggregationContext firstContext = new HistoryAggregationContext(topologyInfo,
                graphWithSettings, false);
        Mockito.when(clock.millis()).thenReturn(firstCheckpoint);
        final PercentileEditorCacheAccess spiedEditor = Mockito.spy(percentileEditor);
        // First initializing history from db.
        spiedEditor.initContext(firstContext, Collections.emptyList());

        percentilePersistenceTasks.clear();
        //First run -  maintenance not executed, reassembly was not executed because its checkpoint = 0
        spiedEditor.completeBroadcast(firstContext);
        //check no maintenance
        Assert.assertEquals(0, percentilePersistenceTasks.stream()
                .filter(t -> t.getStartTimestamp() == 0)
                .count());
        Mockito.verify(spiedEditor, Mockito.times(0)).requestedReassembleFullPage(Mockito.anyLong());
        //change lastCheckpoint for reassembly
        Mockito.when(KV_CONFIG.keyValueStore()
                .get("history-aggregation/" + "fullPageReassemblyLastCheckpoint")).thenReturn(
                Optional.of(String.valueOf(TIMESTAMP_AUG_28_2019_12_00)));

        //One day passed
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        percentilePersistenceTasks.clear();
        //Broadcast after one day - maintenance executed, reassembly was not executed because the period has not passed yet
        spiedEditor.completeBroadcast(firstContext);
        Mockito.verify(spiedEditor, Mockito.never()).requestedReassembleFullPage(Mockito.anyLong());
        final PercentilePersistenceTask maintenanceSaveTask = percentilePersistenceTasks.stream()
                .filter(t -> t.getStartTimestamp() == 0)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(maintenanceSaveTask);
        final ArgumentCaptor<PercentileCounts> percentileCountsCaptor = ArgumentCaptor.forClass(
                PercentileCounts.class);
        //check successful maintenance
        Mockito.verify(maintenanceSaveTask, Mockito.atLeastOnce()).save(
                percentileCountsCaptor.capture(), Mockito.eq(TIMESTAMP_AUG_29_2019_00_00),
                Mockito.any());

        //The period has passed
        percentilePersistenceTasks.clear();
        final long windowInMs = TimeUnit.HOURS.toMillis(24);
        final long yesterdayTimestamp = TIMESTAMP_SEP_5_2019 - windowInMs;
        Mockito.when(clock.millis()).thenReturn(yesterdayTimestamp);
        //Broadcast after the completed period - reassembly executed
        spiedEditor.completeBroadcast(firstContext);
        //The period has passed
        percentilePersistenceTasks.clear();
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_SEP_5_2019);
        //Broadcast after the completed period - reassembly executed
        spiedEditor.completeBroadcast(firstContext);
        // check successful reassembly, i.e. we read all daily blobs
        // First daily has been written successfully
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1))
                        .save(Mockito.any(), Mockito.anyLong(), Mockito.any());
        // 8 blobs read
        for (int i = 1; i
                        < percentilePersistenceTasks.size()
                        - 3; i++) {
            Mockito.verify(percentilePersistenceTasks.get(i), Mockito.times(1))
                            .load(Mockito.any(), Mockito.any(), Mockito.any());
        }
        // full and cleared latest saved
        for (int i = percentilePersistenceTasks.size() - 2; i
                        < percentilePersistenceTasks.size(); i++) {
            Mockito.verify(percentilePersistenceTasks.get(i), Mockito.times(1))
                            .save(Mockito.any(), Mockito.anyLong(), Mockito.any());
        }
        final Integer maxPeriod = spiedEditor.getCache().values().stream().map(
                PercentileCommodityData::getUtilizationCountStore).map(
                UtilizationCountStore::getPeriodDays).max(Long::compare).orElseGet(
                PercentileHistoricalEditorConfig::getDefaultObservationPeriod);
        long reassemblyStartTimestamp = TIMESTAMP_SEP_5_2019 - TimeUnit.DAYS.toMillis(maxPeriod)
                + windowInMs;
        //load all daily
        while (reassemblyStartTimestamp < TIMESTAMP_SEP_5_2019) {
            long finalReassemblyStartTimestamp = reassemblyStartTimestamp;
            final PercentilePersistenceTask task = percentilePersistenceTasks.stream().filter(
                    t -> t.getStartTimestamp() == finalReassemblyStartTimestamp).findAny().orElse(
                    null);
            Assert.assertNotNull(task);
            reassemblyStartTimestamp += windowInMs;
        }
        //checks for previous checkpoint moment page
        final List<PercentilePersistenceTask> tasksForYesterdayTimestamp = getPersistenceTasksFor(yesterdayTimestamp);
        // 1 persist of daily should be executed for previous checkpoint
        Assert.assertEquals(1, tasksForYesterdayTimestamp.size());
        // 1 persist of full should be executed
        Assert.assertEquals(1, getPersistenceTasksFor(0L).size());
        // 1 persist of new empty latest
        Assert.assertEquals(1, getPersistenceTasksFor(TIMESTAMP_SEP_5_2019).size());
    }

    private List<PercentilePersistenceTask> getPersistenceTasksFor(long timestamp) {
        return percentilePersistenceTasks.stream().filter(t -> t.getStartTimestamp() == timestamp)
                        .collect(Collectors.toList());
    }

    /**
     * Tests the occurrence of a notification when maintenance failed.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testMaintenanceNotification() throws InterruptedException, HistoryCalculationException {
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                graphWithSettings, false);
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_SEP_1_2019_12_00);
        final PercentileEditorCacheAccess spiedEditor = Mockito.spy(percentileEditor);
        // First initializing history from db.
        spiedEditor.initContext(context, Collections.emptyList());
        // Throw exception when executing maintenance
        Mockito.when(spiedEditor.createCacheBackup()).thenThrow(HistoryCalculationException.class);

        // Maintenance executed and failed
        spiedEditor.completeBroadcast(context);
        final ArgumentCaptor<List<NotificationDTO>> notificationsCaptor = createArgumentCaptor(
                        List.class);
        Mockito.verify(systemNotificationProducer, Mockito.times(1))
                .sendSystemNotification(notificationsCaptor.capture(), Mockito.any());
        final List<List<NotificationDTO>> notifications = notificationsCaptor.getAllValues();
        Assert.assertEquals(1, notifications.size());
        // Check notification from maintenance failed
        final NotificationDTO maintenanceFailedNotification = notifications.get(0)
                .get(0);
        Assert.assertEquals(
                        String.format("Maintenance failed for '%s' checkpoint, last checkpoint was at '%s'",
                                        Instant.ofEpochMilli(TIMESTAMP_SEP_1_2019_12_00),
                                        Instant.ofEpochMilli(TIMESTAMP_INIT_START_SEP_1_2019)),
                        maintenanceFailedNotification.getDescription());
    }

    @Nonnull
    private static <T> ArgumentCaptor<T> createArgumentCaptor(@Nonnull Class<?> type) {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<T> result = (ArgumentCaptor<T>)ArgumentCaptor.forClass(type);
        return result;
    }

    /**
     * Tests the case were the capacity for percentile data changes and we the lookback period
     * changes. We need to ensure that we used new capacity rather than outdated capacity in DB.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void checkObservationPeriodMaintenanceWindowChanged()
            throws InterruptedException, HistoryCalculationException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        final HistoryAggregationContext firstContext =
            new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        // First initializing history from db.
        percentileEditor.initContext(firstContext, Collections.emptyList());
        // Necessary to set last checkpoint timestamp.
        percentileEditor.completeBroadcast(firstContext);

        // Change the capacity
        UtilizationCountStore store =
            percentileEditor.getCache().get(VCPU_COMMODITY_REFERENCE).getUtilizationCountStore();
        store.addPoints(Collections.singletonList(20d), CAPACITY * 2,
            TIMESTAMP_INIT_START_SEP_1_2019);

        // Change observation periods.
        entitySettings.put(VIRTUAL_MACHINE_OID,
            createEntitySetting(VIRTUAL_MACHINE_OID,
                NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD,
                EntitySettingSpecs.MaxObservationPeriodVirtualMachine));

        // Check observation periods changed.
        final HistoryAggregationContext secondContext =
            new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        percentileEditor.initContext(secondContext,
            Collections.emptyList());

        // Ensure the capacity it the updated one
        Assert.assertThat(store.getLatestCountsRecord().getCapacity(), Matchers.is(CAPACITY * 2));
        PercentileRecord.Builder fullRecord = store.checkpoint(Collections.emptyList(), false);
        Assert.assertThat(fullRecord.getCapacity(), Matchers.is(CAPACITY * 2));
    }

    /**
     * Tests the case were the capacity for percentile data changes and we the lookback period
     * changes. We need to ensure that we used new capacity rather than outdated capacity in DB.
     *
     * @throws InterruptedException when interrupted
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testExpiredOidsDuringCheckPoint()
        throws InterruptedException, HistoryCalculationException, IdentityUninitializedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        final HistoryAggregationContext firstContext =
            new HistoryAggregationContext(topologyInfo, graphWithSettings, false);
        // First initializing history from db.
        percentileEditor.initContext(firstContext, Collections.emptyList());
        // Necessary to set last checkpoint timestamp.
        percentileEditor.completeBroadcast(firstContext);

        // Change the capacity
        PercentileCommodityData vcpuPercentileBeforeExpiration =
            percentileEditor.getCache().get(VCPU_COMMODITY_REFERENCE);
        PercentileCommodityData imageCpuBeforeExpiration =
            percentileEditor.getCache().get(IMAGE_CPU_COMMODITY_REFERENCE);

        Assert.assertNotNull(vcpuPercentileBeforeExpiration);
        Assert.assertNotNull(imageCpuBeforeExpiration);

        LongSet oidsInIdentityCache = new LongOpenHashSet();

        // Simulate the expiration of all the oids besides IMAGE_CPU_COMMODITY_REFERENCE
        oidsInIdentityCache.addAll(Collections.singletonList(IMAGE_CPU_COMMODITY_REFERENCE.getEntityOid()));

        Mockito.when(identityProvider.getCurrentOidsInIdentityCache()).thenReturn(oidsInIdentityCache);

        percentileEditor.completeBroadcast(firstContext);

        PercentileCommodityData vcpuPercentileBAfterExpiratipn =
            percentileEditor.getCache().get(VCPU_COMMODITY_REFERENCE);
        PercentileCommodityData imageCpuAfterExpiration =
            percentileEditor.getCache().get(IMAGE_CPU_COMMODITY_REFERENCE);
        Assert.assertNull(vcpuPercentileBAfterExpiratipn);
        Assert.assertNotNull(imageCpuAfterExpiration);
    }

    /**
     * Tests that the method {@link UtilizationCountStore#addPoints(java.util.List, double, long)},
     * since the timestamp has not changed, even if the latest was cleared.
     */
    @Test
    public void checkAddPointsLatestEmpty()
            throws HistoryCalculationException, InterruptedException {
        final HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                graphWithSettings, false);
        // initializing history from db.
        percentileEditor.initContext(context, Collections.emptyList());
        UtilizationCountStore store = percentileEditor.getCache()
                .get(VCPU_COMMODITY_REFERENCE)
                .getUtilizationCountStore();
        // filling full and latest
        store.addPoints(Collections.singletonList(20d), CAPACITY * 2,
                TIMESTAMP_INIT_START_SEP_1_2019);
        // clear latest
        percentileEditor.getCache()
                .get(VCPU_COMMODITY_REFERENCE)
                .getUtilizationCountStore()
                .checkpoint(Collections.emptyList(), true);
        final List<Integer> oldFullUtilization = store.getFullCountsRecord().getUtilizationList();
        final PercentileRecord.Builder oldLatest = store.getLatestCountsRecord();
        store.addPoints(Collections.singletonList(20d), CAPACITY * 2,
                TIMESTAMP_INIT_START_SEP_1_2019);
        Assert.assertEquals(oldFullUtilization, store.getFullCountsRecord().getUtilizationList());
        Assert.assertEquals(oldLatest, store.getLatestCountsRecord());
    }

    private void checkMaintenance(long periodMsForTotalBlob,
                                  List<List<Integer>> expectedTotalUtilizations,
                                  boolean enforceMaintenanceIsExpected, int dayPersistenceIdx,
                                  int totalPersistenceIdx, long broadcastTimeAfterWindowChanged)
                    throws HistoryCalculationException, InterruptedException {
        // Verify total persistence has been called
        final ArgumentCaptor<PercentileCounts> percentileCountsCaptor =
                        ArgumentCaptor.forClass(PercentileCounts.class);
        final ArgumentCaptor<Long> periodCaptor = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(percentilePersistenceTasks.get(totalPersistenceIdx), Mockito.atLeastOnce())
                .save(percentileCountsCaptor.capture(), periodCaptor.capture(), Mockito.any());
        final List<PercentileCounts> capturedCounts = percentileCountsCaptor.getAllValues();
        final List<Long> capturedPeriods = periodCaptor.getAllValues();
        final PercentileCounts counts = capturedCounts.get(0);
        final List<PercentileRecord> percentileRecords = counts.getPercentileRecordsList();
        final Set<Long> periods =
                        percentileRecords.stream().filter(Objects::nonNull)
                                        .map(r -> (long)r.getPeriod())
                                        .collect(Collectors.toSet());
        Assert.assertThat(capturedPeriods.get(0), CoreMatchers.is(enforceMaintenanceIsExpected
                        ? periodMsForTotalBlob : broadcastTimeAfterWindowChanged));
        final List<List<Integer>> exactUtilizations = percentileRecords.stream()
                        .sorted(Comparator.comparingLong(PercentileRecord::getEntityOid))
                        .map(PercentileRecord::getUtilizationList).collect(Collectors.toList());
        Assert.assertThat(exactUtilizations,
                        CoreMatchers.is(expectedTotalUtilizations));
        Assert.assertThat(periods.size(), CoreMatchers.is(2));
        Assert.assertThat(periods, Matchers.containsInAnyOrder(NEW_BUSINESS_USER_OBSERVATION_PERIOD,
                        NEW_VIRTUAL_MACHINE_OBSERVATION_PERIOD));

        Mockito.verify(percentilePersistenceTasks.get(dayPersistenceIdx), Mockito.atLeastOnce())
            .save(percentileCountsCaptor.capture(), periodCaptor.capture(), Mockito.any());
        checkMaintenance(enforceMaintenanceIsExpected, periodCaptor, capturedCounts.get(1));
    }

    private static void checkMaintenance(boolean enforceMaintenanceIsExpected,
                    ArgumentCaptor<Long> periodCaptor, PercentileCounts maintenanceCurrentDay) {
        final List<PercentileRecord> currentDayRecords =
                        maintenanceCurrentDay.getPercentileRecordsList();
        // after maintenance, latest is cleared and will get saved w/o entries at all
        Assert.assertTrue(currentDayRecords.isEmpty());
        if (enforceMaintenanceIsExpected) {
            final int indexOfYesterdaySave = 1;
            final long period = periodCaptor.getAllValues().get(indexOfYesterdaySave);
            Assert.assertEquals(period, MAINTENANCE_WINDOW_MS);
        }
    }

    private void checkEntityUtilization(List<Integer> utilizations,
                    EntityCommodityFieldReference commodityReference, long newObservationPeriod)
                    throws HistoryCalculationException {
        final PercentileRecord percentileRecord = percentileEditor.getCacheEntry(commodityReference)
                        .getUtilizationCountStore().checkpoint(Collections.emptySet(), true).build();
        Assert.assertEquals(utilizations, percentileRecord.getUtilizationList());
        Assert.assertEquals(newObservationPeriod, percentileRecord.getPeriod());
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast(HistoryAggregationContext)} does nothing if
     * object is not initialized.
     *
     * @throws HistoryCalculationException when failed
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testCompleteBroadcastFailsIfObjectIsNotInitialized()
                    throws HistoryCalculationException, InterruptedException {
        final ArgumentCaptor<Long> periodCaptor = ArgumentCaptor.forClass(Long.class);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.completeBroadcast(context);
        Assert.assertEquals(0, percentilePersistenceTasks.size());
    }

    /**
     * Tests that {@link PercentileEditor.CacheBackup} doesn't restore cache after call of
     * {@link PercentileEditor.CacheBackup#keepCacheOnClose()}.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCacheBackupSuccessCase()
                    throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        final EntityCommodityFieldReference mockReference =
                        Mockito.mock(EntityCommodityFieldReference.class);
        try (PercentileEditor.CacheBackup cacheBackup = new PercentileEditor.CacheBackup<>(
                percentileEditor.getCache(), PercentileCommodityData::new)) {
            percentileEditor.getCache().put(mockReference,
                                            Mockito.mock(PercentileCommodityData.class));
            cacheBackup.keepCacheOnClose();
        }

        Assert.assertTrue(percentileEditor.getCache().containsKey(mockReference));
    }

    /**
     * Tests that {@link PercentileEditor.CacheBackup} restores cache after call of
     * {@link PercentileEditor.CacheBackup#close()}. Also checks that two subsequent calls
     * of {@link PercentileEditor.CacheBackup#close()} doesn't emit a cache backup.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCacheBackupFailureCase() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        final Map<EntityCommodityFieldReference, PercentileCommodityData> originalCache =
                        percentileEditor.getCache();
        final CacheBackup cacheBackup = new CacheBackup<>(originalCache,
                PercentileCommodityData::new);
        final EntityCommodityFieldReference mock =
                        Mockito.mock(EntityCommodityFieldReference.class);
        originalCache.put(mock, Mockito.mock(PercentileCommodityData.class));
        cacheBackup.close();
        Assert.assertFalse(originalCache.containsKey(mock));

        // Check that double close don't override original cache
        originalCache.put(mock, Mockito.mock(PercentileCommodityData.class));
        cacheBackup.close();
        Assert.assertTrue(originalCache.containsKey(mock));
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast(HistoryAggregationContext)} works
     * correctly when half of maintenance windows passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastTooSoon() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_00_00);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_AUG_29_2019_06_00);

        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // We should update LATEST with the latest values we have.
        final ArgumentCaptor<PercentileCounts> latestCaptor =
                        ArgumentCaptor.forClass(PercentileCounts.class);
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(latestCaptor.capture(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));
        final PercentileCounts latest = latestCaptor.getValue();
        Assert.assertEquals(2, latest.getPercentileRecordsList().size());

        Assert.assertThat(percentilePersistenceTasks.size(), CoreMatchers.is(1));

        // We don't update TOTAL because there is not right time now.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.never()).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_AUG_29_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Maintenance shouldn't load any data.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.never())
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG), Mockito.any());
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast(HistoryAggregationContext)} works
     * correctly when one maintenance window passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastOneMaintenanceWindow() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        // last maintenance is now Sep 1 00:00

        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_SEP_1_2019_12_00);
        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // save latest - twice read the outdated - save total - save latest
        Assert.assertEquals(5, percentilePersistenceTasks.size());

        // Save current LATEST day percentiles with one write.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We do exactly one TOTAL write to DB when one maintenance windows passed since last checkpoint.
        Mockito.verify(percentilePersistenceTasks.get(3), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_SEP_1_2019_12_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Save current LATEST day percentiles during maintenance.
        Mockito.verify(percentilePersistenceTasks.get(4), Mockito.times(1)).save(
                Mockito.eq(PercentileCounts.newBuilder().build()),
                Mockito.eq(MAINTENANCE_WINDOW_MS),
                Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // We load exactly two times in maintenance because we have two different periods in graph.
        Mockito.verify(percentilePersistenceTasks.get(1), Mockito.times(1))
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG),
                            Mockito.any());
        Mockito.verify(percentilePersistenceTasks.get(2), Mockito.times(1))
                        .load(Mockito.any(), Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG), Mockito.any());
    }

    /**
     * Tests that {@link PercentileEditor#completeBroadcast(HistoryAggregationContext)} works
     * correctly when several maintenance windows passed since last checkpoint.
     *
     * @throws HistoryCalculationException when something goes wrong
     * @throws InterruptedException when something goes wrong
     */
    @Test
    public void testCompleteBroadcastTwoMaintenanceWindows() throws HistoryCalculationException, InterruptedException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        HistoryAggregationContext context = new HistoryAggregationContext(topologyInfo,
                        graphWithSettings, false);
        percentileEditor.initContext(context, Collections.emptyList());
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_SEP_2_2019_00_00);

        percentilePersistenceTasks.clear();
        percentileEditor.completeBroadcast(context);

        // Update LATEST day once.
        Mockito.verify(percentilePersistenceTasks.get(0), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq((long)(PERCENTILE_HISTORICAL_EDITOR_CONFIG.getMaintenanceWindowHours() * Units.HOUR_MS)),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Normally we should do exactly one TOTAL write even if two maintenance windows passed since last checkpoint.
        Mockito.verify(percentilePersistenceTasks.get(5), Mockito.times(1)).save(Mockito.any(),
                                                    Mockito.eq(TIMESTAMP_SEP_2_2019_00_00),
                                                    Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG));

        // Two periods * two maintenance window = exactly four invocations.
        for (int invocation = 1; invocation < 5; invocation++) {
            Mockito.verify(percentilePersistenceTasks.get(invocation), Mockito.times(1))
                            .load(Mockito.any(),
                                            Mockito.refEq(PERCENTILE_HISTORICAL_EDITOR_CONFIG), Mockito.any());
        }
    }

    /**
     * Checks that exporting and loading of percentile diagnostics is working as expected.
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
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db.
        percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                        false), Collections.emptyList());
        BlobPersistingCachingHistoricalEditorTest.createKvConfig(PercentileHistoricalEditorConfig.STORE_CACHE_TO_DIAGNOSTICS_PROPERTY);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            percentileEditor.collectDiags(output);
            percentileEditor.restoreDiags(output.toByteArray(), null);
        }
        // LATEST(1, 2, 3, 4, 5) + TOTAL(42, 44, 46, 48, 50) = FULL(43, 46, 49, 52, 55)
        Assert.assertEquals(Arrays.asList(43, 46, 49, 52, 55),
                        percentileEditor.getCacheEntry(VCPU_COMMODITY_REFERENCE)
                                        .getUtilizationCountStore()
                                        .checkpoint(Collections.emptySet(), true)
                                        .build()
                                        .getUtilizationList());
        // LATEST(46, 47, 48, 49, 50) + TOTAL(132, 134, 136, 138, 140) = FULL(178, 181, 184, 187, 190)
        Assert.assertEquals(Arrays.asList(178, 181, 184, 187, 190),
                        percentileEditor.getCacheEntry(IMAGE_CPU_COMMODITY_REFERENCE)
                                        .getUtilizationCountStore()
                                        .checkpoint(Collections.emptySet(), true)
                                        .build()
                                        .getUtilizationList());
    }

    /**
     * Checks that binary diags output contains 2 files.
     *
     * @throws InterruptedException in case test has been interrupted
     * @throws HistoryCalculationException in case context initialization failed
     * @throws IOException in case of failure to write bits
     * @throws DiagnosticsException in case of error generating diags
     */
    @Test
    public void testExportOutput() throws HistoryCalculationException, InterruptedException,
        IOException, DiagnosticsException {
        Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
        // First initializing history from db
        percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
            false), Collections.emptyList());
        BlobPersistingCachingHistoricalEditorTest.createKvConfig(PercentileHistoricalEditorConfig.STORE_CACHE_TO_DIAGNOSTICS_PROPERTY);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            percentileEditor.collectDiags(output);
            final Iterable<Diags> diagsReader = new DiagsZipReader(
                new ByteArrayInputStream(output.toByteArray()), null, true);
            final Iterator<Diags> iter = diagsReader.iterator();
            // Checking here that exactly 2 entries exist and are parseable
            Assert.assertTrue(iter.hasNext());
            checkDiags(iter.next());
            Assert.assertTrue(iter.hasNext());
            checkDiags(iter.next());
            Assert.assertFalse(iter.hasNext());
        }
    }

    private static void checkDiags(@Nonnull final Diags diags) throws IOException {
        Assert.assertNotNull(diags.getBytes());
        try(InputStream is = new ByteArrayInputStream(diags.getBytes())) {
            checkPercentileCounts(2, is,
                PercentileDto.PercentileCounts::parseDelimitedFrom);
        }
    }

    private static void checkPercentileCounts(final int expectedCount, @Nonnull final InputStream diags,
          @Nonnull ThrowingFunction<InputStream, PercentileCounts, IOException> parser)
        throws IOException {
        final PercentileCounts counts = parser.apply(diags);
        Assert.assertEquals(expectedCount, counts.getPercentileRecordsList().size());
    }

    /**
     * Tests the case where the entity in the plan has been cloned from another entity. It
     * ensures that the percentile has been set for the entity.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testPercentileEditorPlanContext() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            // ARRANGE
            Mockito.when(clock.millis()).thenReturn(TIMESTAMP_INIT_START_SEP_1_2019);
            // First initializing history from db.
            percentileEditor.initContext(new HistoryAggregationContext(topologyInfo, graphWithSettings,
                false), Collections.emptyList());
            // in our setup the entity with VIRTUAL_MACHINE_OID is a clone of a virtual machine
            // with live entity OID of VIRTUAL_MACHINE_OID_2
            final Map<Long, TopologyEntity.Builder> topologyMap = createTopologyMap();
            final TopologyInfo planTopologyInfo =
                TopologyInfo.newBuilder().setTopologyId(71111L).build();
            final GraphWithSettings graph = new GraphWithSettings(TopologyEntityTopologyGraphCreator
                .newGraph(topologyMap), entitySettings, Collections.emptyMap());
            final HistoryAggregationContext context = new HistoryAggregationContext(planTopologyInfo,
                graph, true);

            List<EntityCommodityReference> commodities = Arrays.asList(
                new EntityCommodityReference(VIRTUAL_MACHINE_OID_2,
                    TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE).build(),
                    null),
                new EntityCommodityFieldReference(BUSINESS_USER_OID_2,
                    TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY)
                        .build(), DESKTOP_POOL_PROVIDER_OID, CommodityField.USED)
            );

            // ACT
            percentileEditor.createPreparationTasks(context, commodities);

            List<? extends Callable<List<Void>>> callableList =
                percentileEditor.createCalculationTasks(context, commodities);

            List<Future<List<Void>>> futureList = executor.invokeAll(callableList);
            for (Future<List<Void>> future : futureList) {
                future.get();
            }

            // ASSERT
            Assert.assertTrue(topologyMap.get(VIRTUAL_MACHINE_OID_2)
                .getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().hasPercentile());
            Assert.assertThat(topologyMap.get(VIRTUAL_MACHINE_OID_2)
                .getEntityBuilder().getCommoditySoldList(0).getHistoricalUsed().getPercentile(),
                Matchers.closeTo(1.0, 0.001));
            Assert.assertTrue(topologyMap.get(BUSINESS_USER_OID_2).getEntityBuilder()
                .getCommoditiesBoughtFromProvidersBuilder(0)
                .getCommodityBought(0)
                .getHistoricalUsed()
                .hasPercentile());
            Assert.assertThat(topologyMap.get(BUSINESS_USER_OID_2).getEntityBuilder()
                    .getCommoditiesBoughtFromProvidersBuilder(0)
                    .getCommodityBought(0)
                    .getHistoricalUsed()
                    .getPercentile(),
                Matchers.closeTo(1.0, 0.001));
        } finally {
            executor.shutdownNow();
        }
    }

    private HashMap<Long, Builder> createTopologyMap() {
        HashMap<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();
        topologyMap.put(VIRTUAL_MACHINE_OID_2, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(VIRTUAL_MACHINE_OID_2)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(
                    TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE))
                        .setCapacity(CAPACITY))
            )
                .setClonedFromEntity(TopologyEntityDTO.newBuilder().setOid(VIRTUAL_MACHINE_OID))
        );

        topologyMap.put(BUSINESS_USER_OID_2, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(BUSINESS_USER_OID_2)
                .setEntityType(EntityType.BUSINESS_USER_VALUE)
                .addCommoditiesBoughtFromProviders(TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(DESKTOP_POOL_PROVIDER_OID)
                    .addCommodityBought(TopologyDTO.CommodityBoughtDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY))))
            )
                .setClonedFromEntity(TopologyEntityDTO.newBuilder().setOid(BUSINESS_USER_OID))
        );

        topologyMap.put(DESKTOP_POOL_PROVIDER_OID, TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(DESKTOP_POOL_PROVIDER_OID)
                .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                .addCommoditySoldList(
                    TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(TopologyDTO
                        .CommodityType.newBuilder().setType(CommodityType.VCPU_VALUE)
                        .setKey(DESKTOP_POOL_COMMODITY_KEY))
                        .setCapacity(CAPACITY))
            )
        );
        return topologyMap;
    }

    /**
     * Access to the percentile editor cached data.
     */
    private static class PercentileEditorCacheAccess extends PercentileEditor {
        PercentileEditorCacheAccess(PercentileHistoricalEditorConfig config,
                StatsHistoryServiceStub statsHistoryClient,
                StatsHistoryServiceBlockingStub statsHistoryBlockingClient,
                Clock clock,
                BiFunction<StatsHistoryServiceStub, Pair<Long, Long>, PercentilePersistenceTask> taskCreator, IdentityProvider identityProvider) {
            super(config, statsHistoryClient, statsHistoryBlockingClient, clock, taskCreator,
                systemNotificationProducer, identityProvider, true);
        }

        PercentileCommodityData getCacheEntry(EntityCommodityFieldReference field) {
            return getCache().get(field);
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileCommodityData> getCache() {
            return super.getCache();
        }
    }

    /**
     * Helper for load/save operations without use to GRPC.
     */
    private static class PercentileTaskStub extends PercentilePersistenceTask {
        // Entity OID -> timestamp -> utilization counts array.
        private static final Map<Long, Map<Long, List<Integer>>> DEFAULT_UTILISATION;
        private static final List<Long> KNOWN_TIMESTAMPS =
                        Arrays.asList(TIMESTAMP_INIT_START_SEP_1_2019, TIMESTAMP_AUG_31_2019_12_00,
                                        TIMESTAMP_AUG_31_2019_00_00, TIMESTAMP_AUG_30_2019_12_00,
                                        TIMESTAMP_AUG_30_2019_00_00, TIMESTAMP_AUG_29_2019_12_00,
                                        TIMESTAMP_AUG_29_2019_00_00, TIMESTAMP_AUG_28_2019_12_00,
                                        PercentilePersistenceTask.TOTAL_TIMESTAMP);
        private final Map<Long, Map<Long, List<Integer>>> currentUtilization;
        private Map<EntityCommodityFieldReference, PercentileRecord> resultForLoad;
        private long lastCheckpointMs;

        static {
            // Virtual Machine - VCPU
            // 41 42 43 44 45 - TOTAL
            // 36 37 38 39 40 - 28 Aug 2019 12:00:00 GMT.
            // 31 32 33 34 35 - 29 Aug 2019 00:00:00 GMT.
            // 26 27 28 29 30 - 29 Aug 2019 12:00:00 GMT.
            // 21 22 23 24 25 - 30 Aug 2019 00:00:00 GMT.
            // 16 14 18 19 20 - 30 Aug 2019 12:00:00 GMT.
            // 11 12 13 14 15 - 31 Aug 2019 00:00:00 GMT.
            //  6  7  8  9 10 - 31 Aug 2019 12:00:00 GMT.
            //  1  2  3  4  5 -  1 Sep 2019 00:00:00 GMT. - LATEST

            // Business User - ImageCPU
            // 86 87 88 89 90 - TOTAL
            // 81 82 83 84 85 - 28 Aug 2019 12:00:00 GMT.
            // 76 77 78 79 80 - 29 Aug 2019 00:00:00 GMT.
            // 71 72 73 74 75 - 29 Aug 2019 12:00:00 GMT.
            // 66 67 68 69 70 - 30 Aug 2019 00:00:00 GMT.
            // 61 62 63 64 65 - 30 Aug 2019 12:00:00 GMT.
            // 56 57 58 59 60 - 31 Aug 2019 00:00:00 GMT.
            // 51 52 53 54 55 - 31 Aug 2019 12:00:00 GMT.
            // 46 47 48 49 50 -  1 Sep 2019 00:00:00 GMT. - LATEST

            DEFAULT_UTILISATION = new HashMap<>();
            int i = 0;
            final List<Long> entities = Arrays.asList(VIRTUAL_MACHINE_OID, BUSINESS_USER_OID);
            for (long entityOid : entities) {
                final Map<Long, List<Integer>> timestampToUtilization = new HashMap<>();
                DEFAULT_UTILISATION.put(entityOid, timestampToUtilization);
                for (long timestamp : KNOWN_TIMESTAMPS) {
                    timestampToUtilization.put(timestamp, Arrays.asList(++i, ++i, ++i, ++i, ++i));
                }
            }
        }

        PercentileTaskStub(StatsHistoryServiceStub unused, @Nonnull  Clock clock, Pair<Long, Long> range) {
            super(unused, clock, range, false);
            currentUtilization = new HashMap<>(DEFAULT_UTILISATION);
            PercentileRecord virtualMachinePercentileRecord = PercentileRecord.newBuilder()
                            .setEntityOid(VIRTUAL_MACHINE_OID)
                            .setCommodityType(VCPU_COMMODITY_REFERENCE.getCommodityType().getType())
                            .setCapacity(CAPACITY)
                            .addCapacityChanges(CapacityChange.newBuilder().setNewCapacity(CAPACITY).setTimestamp(0L))
                            .setPeriod((int)PREVIOUS_VIRTUAL_MACHINE_OBSERVATION_PERIOD)
                            .build();

            PercentileRecord businessUserPercentileRecord = PercentileRecord.newBuilder()
                            .setEntityOid(BUSINESS_USER_OID)
                            .setCommodityType(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getType())
                            .setProviderOid(IMAGE_CPU_COMMODITY_REFERENCE.getProviderOid())
                            .setKey(IMAGE_CPU_COMMODITY_REFERENCE.getCommodityType().getKey())
                            .addCapacityChanges(CapacityChange.newBuilder().setNewCapacity(CAPACITY).setTimestamp(0L))
                            .setCapacity(CAPACITY)
                            .setProviderOid(DESKTOP_POOL_PROVIDER_OID)
                            .setPeriod((int)PREVIOUS_BUSINESS_USER_OBSERVATION_PERIOD)
                            .build();

            resultForLoad = new HashMap<>();
            resultForLoad.put(new EntityCommodityFieldReference(VCPU_COMMODITY_REFERENCE,
                                                                CommodityField.USED),
                              virtualMachinePercentileRecord);
            resultForLoad.put(new EntityCommodityFieldReference(IMAGE_CPU_COMMODITY_REFERENCE, CommodityField.USED),
                              businessUserPercentileRecord);
        }

        public Map<EntityCommodityFieldReference, PercentileRecord> getResultForLoad() {
            return resultForLoad;
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileRecord> load(
            @Nonnull Collection<EntityCommodityReference> commodities,
            @Nonnull PercentileHistoricalEditorConfig config, @Nonnull final Set<Long> oidsToUse) {
            // Current implementation doesn't use this parameter.
            // If this assertion fails then most likely the implementation changed and
            // you should add more unit tests for that.
            Assert.assertTrue(commodities.isEmpty());

            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> entry : resultForLoad
                            .entrySet()) {
                EntityCommodityFieldReference entityCommodityFieldReference = entry.getKey();
                PercentileRecord percentileRecord = entry.getValue();
                final long startTimestamp = KNOWN_TIMESTAMPS.contains(getStartTimestamp()) ?
                                getStartTimestamp() :
                                PercentilePersistenceTask.TOTAL_TIMESTAMP;
                final List<Integer> utilizations =
                                currentUtilization.get(entityCommodityFieldReference.getEntityOid())
                                                .get(startTimestamp);

                if (utilizations == null) {
                    continue;
                }
                entry.setValue(percentileRecord.toBuilder().clearUtilization().addAllUtilization(
                                utilizations).build());
            }
            return Collections.unmodifiableMap(resultForLoad);
        }

        @Override
        public void save(@Nonnull PercentileCounts counts,
                         long periodMs,
                         @Nonnull PercentileHistoricalEditorConfig config) {
            // UNUSED
        }

        @Override
        public long getLastCheckpointMs() {
            return TIMESTAMP_INIT_START_SEP_1_2019;
        }
    }
}
