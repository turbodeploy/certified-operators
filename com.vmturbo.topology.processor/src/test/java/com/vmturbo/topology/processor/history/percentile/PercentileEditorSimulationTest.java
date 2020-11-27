package com.vmturbo.topology.processor.history.percentile;

import static com.vmturbo.topology.processor.history.BaseGraphRelatedTest.createEntitySetting;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
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
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * This class simulates the percentile collection for 60 days when we have a lookback period of
 * 30. After the 60 days, it is verified if the the full record matches last 30 days record.
 */
public class PercentileEditorSimulationTest {
    private static final int SIMULATION_LENGTH_IN_DAYS = 60;
    private static final long PERIOD_START_TIMESTAMP = 1595424732000L;
    private static final long PERIOD_END_TIMESTAMP =
            PERIOD_START_TIMESTAMP + TimeUnit.DAYS.toMillis(SIMULATION_LENGTH_IN_DAYS);
    private static final int LOOK_BACK_PERIOD_30 = 30;
    private static final int LOOK_BACK_PERIOD_7 = 7;
    private static final long BROADCAST_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(60);
    private static final int MAINTENANCE_WINDOW_HOURS = 24;
    private static final long MAINTENANCE_WINDOW_MILLIS =
            TimeUnit.HOURS.toMillis(MAINTENANCE_WINDOW_HOURS);

    private static final KVConfig KV_CONFIG = createKvConfig();

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

    /**
     * The setup to run before running test.
     */
    @Before
    public void setup() {
        // Mock time
        currentTime = PERIOD_START_TIMESTAMP;
        clock = Mockito.mock(Clock.class);
        when(clock.millis()).thenReturn(currentTime);
        config = getConfig(clock);

        persistenceTable = new HashMap<>();
        vmCapacities = new HashMap<>(INITIAL_CAPACITIES);

        percentileEditor = new PercentileEditor(config, null, clock,
            (service, range) -> new PercentileTaskStub(service, range, persistenceTable));

        commodityReferences = VM_OID_TO_MAP.keySet().stream()
            .map(oid -> new EntityCommodityReference(oid,
                TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VCPU_VALUE)
                    .build(), null))
            .collect(Collectors.toList());

        stopwatch = Stopwatch.createStarted();
    }

    /**
     * Prints the execution summary.
     */
    @After
    public void summary() {
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
                is(entry.getValue().serialize(entry.getKey()).getUtilizationList()));
        }
    }

    private Map<EntityCommodityFieldReference, UtilizationCountArray> getLastDaysAggregatedDaysRecord() throws HistoryCalculationException {
        final long lastCheckpoint =
            ((currentTime - BROADCAST_INTERVAL_MILLIS) / MAINTENANCE_WINDOW_MILLIS) * MAINTENANCE_WINDOW_MILLIS;

        // Calculate timestamp for aggregation start
        long startTimestamp = lastCheckpoint
            - TimeUnit.DAYS.toMillis(LOOK_BACK_PERIOD_30)
            + MAINTENANCE_WINDOW_MILLIS;

        // Create a count array for different vms and aggregate the counts for the lookback period
        Map<EntityCommodityFieldReference, UtilizationCountArray> countArrayMap =
            commodityReferences
                .stream()
                .collect(Collectors.toMap(reference -> new EntityCommodityFieldReference(reference, CommodityField.USED),
                    oid -> new UtilizationCountArray(config.getPercentileBuckets(CommodityType.VCPU_VALUE))));

        while (startTimestamp < lastCheckpoint) {
            final Map<EntityCommodityFieldReference, PercentileRecord> page =
                PercentileTaskStub.loadFromCounts(persistenceTable.get(startTimestamp));

            for (Map.Entry<EntityCommodityFieldReference, UtilizationCountArray> entry : countArrayMap.entrySet()) {
                if (page.containsKey(entry.getKey())) {
                    entry.getValue().deserialize(page.get(entry.getKey()), "");
                }
            }

            startTimestamp += MAINTENANCE_WINDOW_MILLIS;
        }

        return countArrayMap;
    }

    private PercentileHistoricalEditorConfig getConfig(Clock clock) {
        return new PercentileHistoricalEditorConfig(1, MAINTENANCE_WINDOW_HOURS, 777777L, 10,
            100,
            ImmutableMap.of(CommodityType.VCPU, ""), KV_CONFIG,
            clock);
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

    private GraphWithSettings createGraphWithSettings(Map<Long, Float> vmCapacities, Map<Long,
        String> vmOidToNames, int lookbackPeriod) {
        Map<Long, TopologyEntity.Builder> topologyBuilderMap = new HashMap<>();
        Map<Long, SettingProto.EntitySettings> entitySettings = new HashMap<>();
        createTopology(topologyBuilderMap, entitySettings, vmCapacities, vmOidToNames, lookbackPeriod);
        final GraphWithSettings graphWithSettings =
                new GraphWithSettings(TopologyEntityTopologyGraphCreator
                .newGraph(topologyBuilderMap),
                entitySettings,
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
                TopologyDTO.TopologyEntityDTO.newBuilder()
                    .setOid(entry.getKey())
                    .setDisplayName(entry.getValue())
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(1L,
                                                PerTargetEntityInformation.newBuilder()
                                                        .setOrigin(EntityOrigin.DISCOVERED)
                                                        .build())
                                        .build())
                                .build()));

            float capacity = vmCapacities.get(entry.getKey());
            float usage = nextUtilizationSupplier.get(entry.getKey()).get() * capacity;

            builder.getEntityBuilder().addCommoditySoldList(TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(CommodityType.VCPU_VALUE)
                    .build())
                .setUsed(usage)
                .setCapacity(capacity)
                .build());

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

    private static KVConfig createKvConfig() {
        final KVConfig result = Mockito.mock(KVConfig.class);
        final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);
        when(result.keyValueStore()).thenReturn(kvStore);
        when(kvStore.get(Mockito.any())).thenReturn(Optional.empty());
        return result;
    }

    /**
     * The persistence object.
     */
    private static class PercentileTaskStub extends PercentilePersistenceTask {
        final Map<Long, PercentileCounts> persistenceTable;

        /**
         * Construct the task to load percentile data for the 'full window' from the persistent store.
         *
         * @param statsHistoryClient persistent store grpc interface
         * @param range              range from start timestamp till end timestamp for which we need
         * @param persistenceTable   the table that we keep the persisted percentile info based
         *                           on timestamp.
         */
        PercentileTaskStub(@Nonnull StatsHistoryServiceGrpc.StatsHistoryServiceStub statsHistoryClient,
                           @Nonnull Pair<Long, Long> range,
                           @Nonnull Map<Long, PercentileCounts> persistenceTable) {
            super(statsHistoryClient, range);
            this.persistenceTable = persistenceTable;
        }

        @Override
        public Map<EntityCommodityFieldReference, PercentileCounts.PercentileRecord> load(
            @Nonnull Collection<EntityCommodityReference> commodities,
            @Nonnull PercentileHistoricalEditorConfig config)  {
            return loadFromCounts(persistenceTable.get(getStartTimestamp()));
        }

        public static Map<EntityCommodityFieldReference, PercentileRecord> loadFromCounts(PercentileCounts counts) {
            final Map<EntityCommodityFieldReference, PercentileRecord> result = new HashMap<>();
            if (counts == null) {
                return Collections.emptyMap();
            }
            for (PercentileRecord record : counts.getPercentileRecordsList()) {
                final TopologyDTO.CommodityType.Builder commTypeBuilder =
                    TopologyDTO.CommodityType.newBuilder().setType(record.getCommodityType());
                if (record.hasKey()) {
                    commTypeBuilder.setKey(record.getKey());
                }
                final Long provider = record.hasProviderOid() ? record.getProviderOid() : null;
                final EntityCommodityFieldReference fieldRef =
                    new EntityCommodityFieldReference(record.getEntityOid(),
                        commTypeBuilder.build(), provider, CommodityField.USED);
                result.put(fieldRef, record);
            }
            return result;
        }

        @Override
        public void save(@Nonnull PercentileCounts counts,
                         long periodMs,
                         @Nonnull PercentileHistoricalEditorConfig config) {
            persistenceTable.put(getStartTimestamp(), counts);
        }
    }
}
