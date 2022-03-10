package com.vmturbo.topology.processor.history.percentile;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.stats.Stats.GetTimestampsRangeRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * This class simulates the percentile collection for 60 days when we have a lookback period of
 * 30. After the 60 days, it is verified if the the full record matches last 30 days record.
 */
public class PercentileEditorSimulationTest extends PercentileBaseTest {
    private static final int SIMULATION_LENGTH_IN_DAYS = 60;
    private static final long PERIOD_START_TIMESTAMP = 1595424732000L;
    private static final long PERIOD_END_TIMESTAMP =
            PERIOD_START_TIMESTAMP + TimeUnit.DAYS.toMillis(SIMULATION_LENGTH_IN_DAYS);
    private static final int LOOK_BACK_PERIOD_30 = 30;
    private static final int LOOK_BACK_PERIOD_7 = 7;
    private static final long BROADCAST_INTERVAL_MILLIS = TimeUnit.HOURS.toMillis(1);
    private static final int MAINTENANCE_WINDOW_HOURS = 24;



    private static final long VM_HIGH_UTILIZED_OID = 45500L;
    private static final long VM_LOW_UTILIZED_OID = 45501L;
    private static final long VM_RANDOM_UTILIZATION_OID = 45502L;
    private static final Map<Long, String> VM_OID_TO_MAP = ImmutableMap.of(
        VM_HIGH_UTILIZED_OID, "Highly Utilized VM",
        VM_LOW_UTILIZED_OID, "Low Utilized VM",
        VM_RANDOM_UTILIZATION_OID, "Randomly Utilized VM"
    );

    private static final Map<Long, Float> INITIAL_CAPACITIES = ImmutableMap.of(
        VM_HIGH_UTILIZED_OID, 1000F,
        VM_LOW_UTILIZED_OID, 2000F,
        VM_RANDOM_UTILIZATION_OID, 3000F);

    private static final float PROBABILITY_OF_CAPACITY_CHANGE = 0.01F;
    private static final float CAPACITY_CHANGE_RATIO_AVG = 0.2F;
    private static final float CAPACITY_CHANGE_RATIO_STD_DEV = 0.05F;

    private static final Logger logger = LogManager.getLogger();

    private Random randomGen = new Random();

    private final Map<Long, Supplier<Float>> nextUtilizationSupplier = ImmutableMap.of(
        VM_HIGH_UTILIZED_OID, () -> getRandomValue(0.9F, 0.05F, 0F, 1F),
        VM_LOW_UTILIZED_OID, () -> getRandomValue(0.1F, 0.05F, 0F, 1F),
        VM_RANDOM_UTILIZATION_OID, () -> randomGen.nextFloat()
    );

    private long currentTime;
    private PercentileHistoricalEditorConfig config;
    private Clock clock;
    private Map<Long, PercentileCounts> persistenceTable;
    private Map<Long, Float> vmCapacities;
    private PercentileEditor percentileEditor;
    private List<EntityCommodityReference> commodityReferences;
    private Stopwatch stopwatch;
    private long checkpointTime;
    private IdentityProvider identityProvider;

    /**
     * The setup to run before running test.
     *
     * @throws IOException when failed
     * @throws IdentityUninitializedException when oids can't be fetched from the cache
     */
    @Before
    public void setup() throws IOException, IdentityUninitializedException {
        history = Mockito.spy(new StatsHistoryServiceMole());
        grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();

        final KVConfig kvConfig = createKvConfig(new HashMap<>());
        // Mock time
        currentTime = PERIOD_START_TIMESTAMP;
        clock = Mockito.mock(Clock.class);
        when(clock.millis()).thenReturn(currentTime);
        config = getConfig(clock, kvConfig);
        persistenceTable = new TreeMap<>();
        vmCapacities = new HashMap<>(INITIAL_CAPACITIES);
        identityProvider = Mockito.mock(IdentityProvider.class);
        Function<GetTimestampsRangeRequest, List<Long>> stampsGetter =
                        (req) -> Lists.newArrayList(persistenceTable.keySet());
        final StatsHistoryServiceStub mockStatsHistoryClient = StatsHistoryServiceGrpc.newStub(grpcServer.getChannel());
        percentileEditor = new PercentileEditor(config, mockStatsHistoryClient,
                        setUpBlockingStub(stampsGetter), clock,
                        (service, range) -> new PercentileTaskStub(service, clock, range, persistenceTable,
                                        (checkpoint) -> {
                                            checkpointTime = checkpoint;
                                        }), Mockito.mock(SystemNotificationProducer.class), identityProvider, false);
        LongSet oidsInCache = new LongOpenHashSet();
        oidsInCache.addAll(VM_OID_TO_MAP.keySet());
        when(identityProvider.getCurrentOidsInIdentityCache()).thenReturn(oidsInCache);
        commodityReferences = VM_OID_TO_MAP.keySet().stream()
            .map(oid -> new EntityCommodityReference(oid,
                new CommodityTypeImpl()
                    .setType(CommodityType.VCPU_VALUE), null))
            .collect(Collectors.toList());

        stopwatch = Stopwatch.createStarted();
    }

    /**
     * Prints the execution summary.
     */
    @After
    public void summary() {
        super.shutdown();
        logger.info("Running the percentile collection simulation took {}", stopwatch);
    }

    /**
     * Runs percentile editor for 60 days worth of percentile data.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testFullRecordValue() throws Exception {
        for (; currentTime < PERIOD_END_TIMESTAMP; currentTime += BROADCAST_INTERVAL_MILLIS) {
            when(clock.millis()).thenReturn(currentTime);
            GraphWithSettings graphWithSettings = createGraphWithSettings(vmCapacities,
                VM_OID_TO_MAP, LOOK_BACK_PERIOD_30);
            final HistoryAggregationContext context =
                new HistoryAggregationContext(TopologyDTO.TopologyInfo.newBuilder().setTopologyId(77777L).build(),
                    graphWithSettings, false);
            percentileEditor.initContext(context, commodityReferences);
            percentileEditor.createPreparationTasks(context, commodityReferences);
            List<? extends Callable<List<Void>>> calculations =
                percentileEditor.createCalculationTasks(context, commodityReferences);
            for (Callable<List<Void>> callable : calculations) {
                callable.call();
            }
            percentileEditor.completeBroadcast(context);
            updateCapacities(vmCapacities, VM_OID_TO_MAP, currentTime);
        }
        percentileEditor.requestedReassembleFullPage(currentTime);

        // Verify the full and the records in the lookback period match
        verifyTheFullRecord();
    }

    /**
     * Runs percentile editor for 60 days worth of percentile data. After 15 days the lookback
     * period changes to 30 days.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testFullRecordValueWithMaintenanceWindowChange() throws Exception {
        Mockito.doReturn(0).when(config).getFullPageReassemblyPeriodInDays();
        final long timestampForObservationPeriodChange = currentTime + TimeUnit.DAYS.toMillis(15)
            + TimeUnit.HOURS.toMillis(6);

        for (; currentTime < PERIOD_END_TIMESTAMP; currentTime += BROADCAST_INTERVAL_MILLIS) {
            when(clock.millis()).thenReturn(currentTime);
            int lookBackPeriod = currentTime < timestampForObservationPeriodChange
                ? LOOK_BACK_PERIOD_7 : LOOK_BACK_PERIOD_30;

            GraphWithSettings graphWithSettings = createGraphWithSettings(vmCapacities,
                VM_OID_TO_MAP, lookBackPeriod);
            final HistoryAggregationContext context =
                new HistoryAggregationContext(TopologyDTO.TopologyInfo.newBuilder().setTopologyId(77777L).build(),
                    graphWithSettings, false);
            percentileEditor.initContext(context, commodityReferences);
            percentileEditor.createPreparationTasks(context, commodityReferences);
            List<? extends Callable<List<Void>>> calculations =
                percentileEditor.createCalculationTasks(context, commodityReferences);
            for (Callable<List<Void>> callable : calculations) {
                callable.call();
            }
            percentileEditor.completeBroadcast(context);
            updateCapacities(vmCapacities, VM_OID_TO_MAP, currentTime);
        }

        // Verify the full and the records in the lookback period match
        verifyTheFullRecord();
    }

    private void verifyTheFullRecord() throws HistoryCalculationException {
        Map<EntityCommodityFieldReference, UtilizationCountArray> countArrayMap =
            getLastDaysAggregatedDaysRecord();

        // compare the calculated count values to full record
        for (Map.Entry<EntityCommodityFieldReference, UtilizationCountArray> entry : countArrayMap.entrySet()) {
            // get the counts from the persisted full record
            PercentileRecord record = persistenceTable.get(0L).getPercentileRecordsList()
                .stream()
                .filter(rec -> rec.getEntityOid() == entry.getKey().getEntityOid())
                .findAny()
                .get();
            logger.info("Verifying the percentile records for {}",
                VM_OID_TO_MAP.get(entry.getKey().getEntityOid()));
            Assert.assertThat(record.getUtilizationList(),
                            Matchers.is(entry.getValue().serialize(entry.getKey())
                                            .getUtilizationList()));
        }
    }

    private Map<EntityCommodityFieldReference, UtilizationCountArray> getLastDaysAggregatedDaysRecord() throws HistoryCalculationException {
        // Create a count array for different vms and aggregate the counts for the lookback period
        Map<EntityCommodityFieldReference, UtilizationCountArray> countArrayMap =
            commodityReferences
                .stream()
                .collect(Collectors.toMap(reference -> new EntityCommodityFieldReference(reference, CommodityField.USED),
                    oid -> new UtilizationCountArray(config.getPercentileBuckets(CommodityType.VCPU_VALUE))));
        final PercentileCounts fullCounts = persistenceTable.get(0L);
        final Map<Long, Integer> oidToPeriod = fullCounts.getPercentileRecordsList().stream()
                        .collect(Collectors.toMap(PercentileRecord::getEntityOid,
                                        PercentileRecord::getPeriod));
        final boolean latestIncludedInFull =
                        persistenceTable.get(checkpointTime).getPercentileRecordsCount() == 0;
        // exclude the persisted full and latest after full's moment
        for (Entry<Long, PercentileCounts> entry : persistenceTable.entrySet()) {
            final Long startTimestamp = entry.getKey();
            if (startTimestamp == 0L || startTimestamp >= checkpointTime) {
                continue;
            }
            final PercentileCounts counts = entry.getValue();
            if (counts.getPercentileRecordsCount() <= 0) {
                continue;
            }
            final Map<EntityCommodityFieldReference, PercentileRecord> page =
                            PercentileTaskStub.loadFromCounts(persistenceTable.get(startTimestamp));

            for (Map.Entry<EntityCommodityFieldReference, UtilizationCountArray> refToArray : countArrayMap
                            .entrySet()) {
                final PercentileRecord record = page.get(refToArray.getKey());
                final Integer rawPeriod = oidToPeriod.get(record.getEntityOid());
                if (rawPeriod == null) {
                    continue;
                }
                final Integer period = latestIncludedInFull
                                ?
                                rawPeriod + 1
                                :
                                rawPeriod;
                if (startTimestamp >= checkpointTime - TimeUnit.DAYS.toMillis(period)) {
                    refToArray.getValue().deserialize(record, "");
                }
            }
        }

        return countArrayMap;
    }

    private static PercentileHistoricalEditorConfig getConfig(Clock clock, KVConfig kvConfig) {
        return Mockito.spy(new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 777777L, 10,
            100,
            ImmutableMap.of(CommodityType.VCPU, ""), kvConfig,
            clock));
    }

    private void updateCapacities(Map<Long, Float> vmCapacities, Map<Long, String> vmOidToNames,
                                  long currentTime) {
        for (Map.Entry<Long, String> entry : vmOidToNames.entrySet()) {
            if (randomGen.nextFloat() < PROBABILITY_OF_CAPACITY_CHANGE) {
                float ratioToChange = getRandomValue(CAPACITY_CHANGE_RATIO_AVG,
                    CAPACITY_CHANGE_RATIO_STD_DEV, 0.1F, 0.4F);
                float newCapacity;
                if (randomGen.nextFloat() < 0.5) {
                    newCapacity = vmCapacities.get(entry.getKey()) * (1 - ratioToChange);
                } else {
                    newCapacity = vmCapacities.get(entry.getKey()) * (1 + ratioToChange);
                }
                newCapacity = Math.max(INITIAL_CAPACITIES.get(entry.getKey()) * 0.5F, newCapacity);
                newCapacity = Math.min(INITIAL_CAPACITIES.get(entry.getKey()) * 2F, newCapacity);
                vmCapacities.put(entry.getKey(), newCapacity);
                logger.info("The capacity for {} changed to {} at {}.",
                    vmOidToNames.get(entry.getKey()), newCapacity, currentTime);
            }
        }
    }

    private GraphWithSettings createGraphWithSettings(Map<Long, Float> vmCapacities,
                                                      Map<Long, String> vmOidToNames, int lookbackPeriod) {
        final Map<Long, EntitySettings> entitySettings = new HashMap<>();
        Map<Long, TopologyEntity.Builder> topologyBuilderMap = new HashMap<>();
        createTopology(topologyBuilderMap, entitySettings, vmCapacities, vmOidToNames, lookbackPeriod);
        final GraphWithSettings graphWithSettings =
                new GraphWithSettings(TopologyEntityTopologyGraphCreator
                .newGraph(topologyBuilderMap), entitySettings,
                Collections.emptyMap());

        return graphWithSettings;
    }

    private void createTopology(Map<Long, TopologyEntity.Builder> topologyBuilderMap,
                                Map<Long, SettingProto.EntitySettings> entitySettings,
                                Map<Long, Float> vmCapacities,
                                Map<Long, String> vmOidToNames, int lookbackPeriod) {

        createEntities(topologyBuilderMap, vmCapacities, vmOidToNames);

        for (Map.Entry<Long, String> entry : vmOidToNames.entrySet()) {
            entitySettings.put(entry.getKey(),
                createEntitySetting(entry.getKey(), lookbackPeriod,
                    EntitySettingSpecs.MaxObservationPeriodVirtualMachine));
        }
    }

    private void createEntities(Map<Long, TopologyEntity.Builder> topologyBuilderMap, Map<Long, Float> vmCapacities, Map<Long, String> vmOidToNames) {
        for (Map.Entry<Long, String> entry : vmOidToNames.entrySet()) {
            TopologyEntity.Builder builder = TopologyEntity.newBuilder(
                new TopologyEntityImpl()
                    .setOid(entry.getKey())
                    .setDisplayName(entry.getValue())
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOrigin(new OriginImpl()
                                .setDiscoveryOrigin(new DiscoveryOriginImpl()
                                        .putDiscoveredTargetData(1L,
                                                new PerTargetEntityInformationImpl()
                                                        .setOrigin(EntityOrigin.DISCOVERED)))));

            float capacity = vmCapacities.get(entry.getKey());
            float usage = nextUtilizationSupplier.get(entry.getKey()).get() * capacity;

            builder.getTopologyEntityImpl().addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(new CommodityTypeImpl()
                    .setType(CommodityType.VCPU_VALUE))
                .setUsed(usage)
                .setCapacity(capacity));

            topologyBuilderMap.put(entry.getKey(), builder);
        }
    }

    private float getRandomValue(float average, float stdDeviation, float min, float max) {
        float randomValue = average + stdDeviation * (float)randomGen.nextGaussian();
        // cap bottom
        randomValue = Math.max(min, randomValue);
        // cap top
        randomValue = Math.min(max, randomValue);
        return randomValue;
    }

    private static KVConfig createKvConfig(Map<String, Object> fakeKVStore) {
        final KVConfig result = Mockito.mock(KVConfig.class);
        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);
        when(result.keyValueStore()).thenReturn(kvStore);
        when(kvStore.get(Mockito.any())).then(invocation -> Optional
                        .ofNullable(fakeKVStore.get(invocation.getArgumentAt(0, String.class))));
        Mockito.doAnswer(invocation -> fakeKVStore.put(invocation.getArgumentAt(0, String.class),
                        invocation.getArgumentAt(1, Object.class))).when(kvStore)
                        .put(Mockito.any(), Mockito.any());
        return result;
    }

    /**
     * The persistence object.
     */
    private static class PercentileTaskStub extends PercentilePersistenceTask {
        final Map<Long, PercentileCounts> persistenceTable;
        final Consumer<Long> checkpointSetter;

        /**
         * Construct the task to load percentile data for the 'full window' from the persistent store.
         *
         * @param statsHistoryClient persistent store grpc interface
         * @param clock              The clock for timing
         * @param range              range from start timestamp till end timestamp for which we need
         * @param persistenceTable   the table that we keep the persisted percentile info based
         *                           on timestamp.
         * @param checkpointSetter   consumer to update the latest checkpoint moment
         */
        PercentileTaskStub(@Nonnull StatsHistoryServiceGrpc.StatsHistoryServiceStub statsHistoryClient,
                           @Nonnull Clock clock,
                           @Nonnull Pair<Long, Long> range,
                           @Nonnull Map<Long, PercentileCounts> persistenceTable,
                           @Nonnull Consumer<Long> checkpointSetter) {
            super(statsHistoryClient, clock, range, false);
            this.persistenceTable = persistenceTable;
            this.checkpointSetter = checkpointSetter;
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileCounts.PercentileRecord> load(
            @Nonnull Collection<EntityCommodityReference> commodities,
            @Nonnull PercentileHistoricalEditorConfig config, @Nonnull final Set<Long> oidsToUse)  {
            return loadFromCounts(persistenceTable.get(getStartTimestamp()));
        }

        public static Map<EntityCommodityFieldReference, PercentileRecord> loadFromCounts(PercentileCounts counts) {
            final Map<EntityCommodityFieldReference, PercentileRecord> result = new HashMap<>();
            if (counts == null) {
                return Collections.emptyMap();
            }
            for (PercentileRecord record : counts.getPercentileRecordsList()) {
                final CommodityTypeImpl commType =
                    new CommodityTypeImpl().setType(record.getCommodityType());
                if (record.hasKey()) {
                    commType.setKey(record.getKey());
                }
                final Long provider = record.hasProviderOid() ? record.getProviderOid() : null;
                final EntityCommodityFieldReference fieldRef =
                    new EntityCommodityFieldReference(record.getEntityOid(),
                        commType, provider, CommodityField.USED);
                result.put(fieldRef, record);
            }
            return result;
        }

        @Override
        public void save(@Nonnull PercentileCounts counts,
                         long periodMs,
                         @Nonnull PercentileHistoricalEditorConfig config) {
            persistenceTable.put(getStartTimestamp(), counts);
            if (getStartTimestamp() == 0L) {
                checkpointSetter.accept(periodMs);
            }
        }
    }
}
