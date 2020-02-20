package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.RelationType.COMMODITIES;
import static com.vmturbo.history.schema.RelationType.COMMODITIESBOUGHT;
import static com.vmturbo.history.stats.StatsTestUtils.expectedStatRecord;
import static com.vmturbo.history.stats.StatsTestUtils.newMarketStatRecordWithEntityType;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKey;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKeyAndEffectiveCapacity;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithProducerUuid;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.jooq.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.BuStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.snapshots.SharedPropertyPopulator;
import com.vmturbo.history.stats.snapshots.SnapshotCreator;
import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;

public class StatSnapshotCreatorTest {

    private static final float CAPACITY = 1000F;
    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);
    private static final String C_1 = "c1";
    private static final String C_2 = "c2";
    private static final String C_1_SUBTYPE = "c1-subtype";
    private static final String C_2_SUBTYPE = "c2-subtype";
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String USED = PropertySubType.Used.getApiParameterName();
    public static final float FLOAT_COMPARISON_EPSILON = 0.001F;
    private static final long PRODUCER_OID = 111L;
    private static final String PROVIDER_DISPLAY_NAME = "providerDisplayName";
    private static final String COMMODITY_KEY = "commodityKey";
    private static final String TEST_RECORD_KEY = "testRecord";
    private static final float HIST_UTILIZATION_CAPACITY = 1500F;

    private LiveStatsReader liveStatsReader = Mockito.mock(LiveStatsReader.class);
    private StatSnapshotCreator snapshotCreator = StatsConfig.createStatSnapshotCreator(liveStatsReader);

    @Test
    public void testGroupByKey() {
        final List<CommodityRequest> commodityRequests =
                Collections.singletonList(CommodityRequest.newBuilder()
                        .setCommodityName(C_1)
                        .addGroupBy(StringConstants.KEY)
                        .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
                newStatRecordWithKey(SNAPSHOT_TIME, 1, C_1, C_1_SUBTYPE, KEY_1),
                newStatRecordWithKey(SNAPSHOT_TIME, 2, C_1, C_1_SUBTYPE, KEY_2));

        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        final Collection<String> statRecordsList =
                        snapshot.getStatRecordsList().stream().map(StatRecord::getStatKey)
                                        .collect(Collectors.toSet());
        assertThat(statRecordsList, containsInAnyOrder(KEY_1, KEY_2));
    }

    /**
     * Test group by relatedEntity for StatSnapshotCreator. It tests the scenario that one
     * LogicalPool consumes two different disk arrays. We expect to see two set of bought
     * commodities in the result.
     *
     * UI usually passes commodity request filters with all groupBy fields:
     *     commodityName: StorageAmount, groupBy: [key, relatedEntity, virtualDisk]
     *     commodityName: StorageAccess, groupBy: [key, relatedEntity, virtualDisk]
     * Only relatedEntity has effect here, but the others are also added in this test to ensure
     * they don't have effect on the result. This test will verify that there are two StorageAmount
     * and two StorageAccess (related to different provider) with different used value in result.
     */
    @Test
    public void testGroupByRelatedEntity() {
        final String oidDiskArray1 = "111";
        final String oidDiskArray2 = "112";
        final String storageAmount = UICommodityType.STORAGE_AMOUNT.apiStr();
        final String storageAccess = UICommodityType.STORAGE_ACCESS.apiStr();
        final float delta = 0.000001f;

        when(liveStatsReader.getEntityDisplayNameForId(any())).thenReturn("dummyDisplayName");

        // only relatedEntity has effect here, although ui passes all groupBy filters
        final List<CommodityRequest> commodityRequests = ImmutableList.of(
            CommodityRequest.newBuilder()
                .setCommodityName(storageAmount)
                .addGroupBy(StringConstants.KEY)
                .addGroupBy(StringConstants.RELATED_ENTITY)
                .addGroupBy(StringConstants.VIRTUAL_DISK)
                .build(),
            CommodityRequest.newBuilder()
                .setCommodityName(storageAccess)
                .addGroupBy(StringConstants.KEY)
                .addGroupBy(StringConstants.RELATED_ENTITY)
                .addGroupBy(StringConstants.VIRTUAL_DISK)
                .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 12, storageAmount, USED, oidDiskArray1),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 13, storageAccess, USED, oidDiskArray1),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 14, storageAmount, USED, oidDiskArray2),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 15, storageAccess, USED, oidDiskArray2)
        );

        final List<StatSnapshot> snapshots =
            snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());

        assertThat(snapshots.size(), is(1));
        List<StatRecord> statRecordsList = snapshots.get(0).getStatRecordsList();

        // check that there are 4 records in response
        assertThat(statRecordsList.size(), is(4));

        // stats by provider uuid and then by commodity name
        final Map<String, Map<String, StatRecord>> map = statRecordsList.stream()
            .collect(Collectors.groupingBy(StatRecord::getProviderUuid,
                Collectors.toMap(StatRecord::getName, Function.identity())));

        assertEquals(map.get(oidDiskArray1).get(storageAmount).getUsed().getAvg(), 12, delta);
        assertEquals(map.get(oidDiskArray1).get(storageAccess).getUsed().getAvg(), 13, delta);
        assertEquals(map.get(oidDiskArray2).get(storageAmount).getUsed().getAvg(), 14, delta);
        assertEquals(map.get(oidDiskArray2).get(storageAccess).getUsed().getAvg(), 15, delta);
    }

    /**
     * Test group by virtualDisk for StatSnapshotCreator. It tests the scenario that one VM buys
     * two set of commodities from same StorageTier (each set is related to a different volume).
     * Volume id is stored in the commodity key field. We expect to see two set of bought
     * commodities with different keys in the result.
     *
     * UI usually passes commodity request filters with all groupBy fields:
     *     commodityName: StorageAmount, groupBy: [key, relatedEntity, virtualDisk]
     *     commodityName: StorageAccess, groupBy: [key, relatedEntity, virtualDisk]
     * Only virtualDisk has effect here, but the others are also added in this test to ensure
     * they don't have effect on the result. This test will verify that there are two StorageAmount
     * and two StorageAccess (related to different volume) with different used value in result.
     */
    @Test
    public void testGroupByVirtualDisk() {
        final String volume1 = "111";
        final String volume2 = "112";
        final String storageAmount = UICommodityType.STORAGE_AMOUNT.apiStr();
        final String storageAccess = UICommodityType.STORAGE_ACCESS.apiStr();
        final float delta = 0.000001f;

        when(liveStatsReader.getEntityDisplayNameForId(any())).thenReturn("dummyDisplayName");

        // only virtualDisk has effect here, although ui passes all groupBy filters
        final List<CommodityRequest> commodityRequests = ImmutableList.of(
            CommodityRequest.newBuilder()
                .setCommodityName(storageAmount)
                .addGroupBy(StringConstants.KEY)
                .addGroupBy(StringConstants.RELATED_ENTITY)
                .addGroupBy(StringConstants.VIRTUAL_DISK)
                .build(),
            CommodityRequest.newBuilder()
                .setCommodityName(storageAccess)
                .addGroupBy(StringConstants.KEY)
                .addGroupBy(StringConstants.RELATED_ENTITY)
                .addGroupBy(StringConstants.VIRTUAL_DISK)
                .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecordWithKey(SNAPSHOT_TIME, 12, storageAmount, USED, volume1),
            newStatRecordWithKey(SNAPSHOT_TIME, 13, storageAccess, USED, volume1),
            newStatRecordWithKey(SNAPSHOT_TIME, 14, storageAmount, USED, volume2),
            newStatRecordWithKey(SNAPSHOT_TIME, 15, storageAccess, USED, volume2)
        );

        final List<StatSnapshot> snapshots =
            snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());

        assertThat(snapshots.size(), is(1));
        List<StatRecord> statRecordsList = snapshots.get(0).getStatRecordsList();

        // check that there are 4 records in response
        assertThat(statRecordsList.size(), is(4));

        // stats by virtualDisk id (which is commodity key) and then by commodity name
        final Map<String, Map<String, StatRecord>> map = statRecordsList.stream()
            .collect(Collectors.groupingBy(StatRecord::getStatKey,
                Collectors.toMap(StatRecord::getName, Function.identity())));

        assertEquals(map.get(volume1).get(storageAmount).getUsed().getAvg(), 12, delta);
        assertEquals(map.get(volume1).get(storageAccess).getUsed().getAvg(), 13, delta);
        assertEquals(map.get(volume2).get(storageAmount).getUsed().getAvg(), 14, delta);
        assertEquals(map.get(volume2).get(storageAccess).getUsed().getAvg(), 15, delta);
    }

    @Test
    public void testNoGroupByKey() {
        // arrange
        final List<CommodityRequest> commodityRequests =
            Collections.singletonList(CommodityRequest.newBuilder()
                .setCommodityName(C_1)
                .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecordWithKey(SNAPSHOT_TIME, 1, C_1, C_1_SUBTYPE, KEY_1),
            newStatRecordWithKey(SNAPSHOT_TIME, 2, C_1, C_1_SUBTYPE, KEY_2));

        final List<StatSnapshot> snapshots =
            snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());

        Assert.assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        Assert.assertThat(snapshot.getStatRecordsList().size(), CoreMatchers.is(1));
        Assert.assertThat(snapshot.getStatRecordsList().iterator().next().getStatKey(), Matchers.anyOf(
                        CoreMatchers.is(KEY_1), CoreMatchers.is(KEY_2)));
    }

    /**
     * Test commodity percentile utilization.
     */
    @Test
    public void testPercentileUtilization() {
        Mockito.when(liveStatsReader.getEntityDisplayNameForId(Mockito.eq(PRODUCER_OID)))
                        .thenReturn(PROVIDER_DISPLAY_NAME);
        // arrange
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
                        CommodityRequest.newBuilder()
                                        .setCommodityName(UICommodityType.IMAGE_CPU.apiStr())
                                        .build());
        final BuStatsLatestRecord usedRecord = new BuStatsLatestRecord();
        usedRecord.setSnapshotTime(SNAPSHOT_TIME);
        usedRecord.setPropertyType(UICommodityType.IMAGE_CPU.apiStr());
        usedRecord.setPropertySubtype(StringConstants.PROPERTY_SUBTYPE_USED);
        usedRecord.setRelation(RelationType.COMMODITIESBOUGHT);
        final String userRecordCommodityKey = "userRecordCommodityKey";
        usedRecord.setCommodityKey(userRecordCommodityKey);
        usedRecord.setAvgValue(500D);
        usedRecord.setMinValue(500D);
        usedRecord.setMaxValue(500D);
        final double capacity = 1000D;
        usedRecord.setCapacity(capacity);

        final HistUtilizationRecord percentileRecord = new HistUtilizationRecord();
        percentileRecord.setPropertyTypeId(UICommodityType.IMAGE_CPU.typeNumber());
        percentileRecord.setUtilization(BigDecimal.valueOf(0.4D));
        percentileRecord.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        percentileRecord.setCapacity(capacity);
        percentileRecord.setCommodityKey("percentileCommodityKey");
        percentileRecord.setPropertySlot(0);
        percentileRecord.setValueType(HistoryUtilizationType.Percentile.ordinal());

        final List<StatSnapshot> statSnapshots =
                snapshotCreator.createStatSnapshots(Arrays.asList(percentileRecord, usedRecord),
                        false, commodityRequests)
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());
        Assert.assertEquals(1, statSnapshots.size());
        Assert.assertEquals(1, statSnapshots.get(0).getStatRecordsList().size());
        final StatRecord statRecord = statSnapshots.get(0).getStatRecords(0);
        // Checks that stat record values take precedence on hist utilization values
        Assert.assertThat(statRecord.getStatKey(), CoreMatchers.is(userRecordCommodityKey));
        Assert.assertEquals(500D, statRecord.getUsed().getAvg(), 0.0001);
        final HistUtilizationValue percentileValue =
                        statRecord.getHistUtilizationValueList().stream()
                                        .filter(value -> HistoryUtilizationType.Percentile
                                                        .getApiParameterName()
                                                        .equals(value.getType())).findAny().get();
        Assert.assertEquals(400D, percentileValue.getUsage().getAvg(), 0.0001);
        Assert.assertEquals(capacity, percentileValue.getCapacity().getAvg(), 0.0001);
    }

    /**
     * Test that solely hist utilization record returned from database will populate all possible
     * fields in {@link StatRecord} that can be deduced from {@link HistUtilizationRecord}.
     */
    @Test
    public void testSolelyHistUtilizationRecord() {

        Mockito.when(liveStatsReader.getEntityDisplayNameForId(Mockito.eq(PRODUCER_OID)))
                        .thenReturn(PROVIDER_DISPLAY_NAME);
        final double capacity = 1000D;
        final HistUtilizationRecord histUtilizationRecord = new HistUtilizationRecord();
        histUtilizationRecord.setPropertyTypeId(UICommodityType.IMAGE_CPU.typeNumber());
        histUtilizationRecord.setUtilization(BigDecimal.valueOf(0.4D));
        histUtilizationRecord.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        histUtilizationRecord.setCommodityKey(COMMODITY_KEY);
        histUtilizationRecord.setCapacity(capacity);
        histUtilizationRecord.setPropertySlot(0);
        histUtilizationRecord.setValueType(HistoryUtilizationType.Percentile.ordinal());
        histUtilizationRecord.setProducerOid(PRODUCER_OID);

        final List<StatSnapshot> statSnapshots = snapshotCreator
                        .createStatSnapshots(Collections.singletonList(histUtilizationRecord), false,
                                        Collections.emptyList()).map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());
        Assert.assertEquals(1, statSnapshots.size());
        final StatSnapshot statSnapshot = statSnapshots.get(0);
        Assert.assertEquals(1, statSnapshot.getStatRecordsList().size());
        final StatRecord statRecord = statSnapshot.getStatRecords(0);
        Assert.assertThat(statRecord.getName(),
                        CoreMatchers.is(UICommodityType.IMAGE_CPU.apiStr()));
        Assert.assertThat(statRecord.getProviderUuid(),
                        CoreMatchers.is(String.valueOf(PRODUCER_OID)));
        Assert.assertThat(statRecord.getProviderDisplayName(),
                        CoreMatchers.is(PROVIDER_DISPLAY_NAME));
        Assert.assertThat(statRecord.getStatKey(), CoreMatchers.is(COMMODITY_KEY));
        Assert.assertThat(statRecord.getUnits(),
                        CoreMatchers.is(CommodityTypeUnits.IMAGE_CPU.getUnits()));

        final HistUtilizationValue percentileValue =
                        statRecord.getHistUtilizationValueList().stream()
                                        .filter(value -> HistoryUtilizationType.Percentile
                                                        .getApiParameterName()
                                                        .equals(value.getType())).findAny().get();
        Assert.assertEquals(400D, percentileValue.getUsage().getAvg(), 0.0001);
        Assert.assertEquals(capacity, percentileValue.getCapacity().getAvg(), 0.0001);
    }

    /**
     * Test that time slot values populated correctly.
     */
    @Test
    public void testTimeSlotUtilization() {
        final float capacity = CAPACITY;
        final List<Record> dbRecords = IntStream.of(1, 2, 3)
                        .mapToObj(slot -> createRecord(capacity, slot / 10D, slot))
                        .collect(Collectors.toList());

        final List<StatSnapshot> statSnapshots = snapshotCreator
                        .createStatSnapshots(dbRecords, false, Collections.emptyList())
                        .map(StatSnapshot.Builder::build).collect(Collectors.toList());
        Assert.assertEquals(1, statSnapshots.size());
        Assert.assertEquals(1, statSnapshots.get(0).getStatRecordsList().size());
        final StatRecord statRecords = statSnapshots.get(0).getStatRecords(0);
        final List<HistUtilizationValue> timeSlotHistoryValues =
                        statRecords.getHistUtilizationValueList().stream()
                                        .filter(value -> HistoryUtilizationType.Timeslot
                                                        .getApiParameterName()
                                                        .equals(value.getType()))
                                        .collect(Collectors.toList());
        final List<Float> timeSlotValues =
                        timeSlotHistoryValues.stream().map(HistUtilizationValue::getUsage)
                                        .map(StatValue::getAvg).collect(Collectors.toList());
        final Collection<Float> capacities =
                        timeSlotHistoryValues.stream().map(HistUtilizationValue::getCapacity)
                                        .map(StatValue::getAvg).collect(Collectors.toSet());
        Assert.assertThat(timeSlotValues, Matchers.is(ImmutableList.of(100F, 200F, 300F)));
        Assert.assertThat(capacities.size(), CoreMatchers.is(1));
        Assert.assertThat(capacities.iterator().next(), CoreMatchers.is(capacity));
    }

    private static Record createRecord(double capacity, double utilization, int slot) {
        final HistUtilizationRecord result = new HistUtilizationRecord();
        result.setPropertyTypeId(UICommodityType.POOL_CPU.typeNumber());
        result.setUtilization(BigDecimal.valueOf(utilization));
        result.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        result.setCapacity(capacity);
        result.setPropertySlot(slot);
        result.setValueType(HistoryUtilizationType.Timeslot.ordinal());
        result.setProducerOid(PRODUCER_OID);
        return result;
    }

    @Test
    public void testEffectiveCapacityCalculation() {
        // verify that the effective capacity calculation in the stat snapshot is behaving as expected
        // we need a non-mocked stat record builder to do this.
        final List<Record> statsRecordsList = Lists.newArrayList(
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 1, 1.0, C_1, C_1_SUBTYPE, KEY_1),
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 1, 0.5, C_1, C_1_SUBTYPE, KEY_1),
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 2, 0.5, C_1, C_1_SUBTYPE, KEY_1));

        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, Collections.emptyList())
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        Assert.assertEquals(1, snapshots.size());
        // Total capacity should be (3+3+6) = 12, and total effective capacity should be (3+1.5+3) = 7.5
        // so we expect the "reserved" amount to be (capacity - effective capacity) = (4 - 2.5) = 1.5
        Assert.assertEquals(4.5, snapshots.get(0).getStatRecords(0).getReserved(), 0);
    }

    /**
     * Tests the VMem capacity value generated by two records from hist_utilization and
     * vm_stats_latest is not be aggregated in one StatsAccumulator and is counted separately for
     * each table.
     */
    @Test
    public void testVMemCapacityCalculation() {
        final Multimap<String, Record> multimap = HashMultimap.create();
        final HistUtilizationRecord result = new HistUtilizationRecord();
        result.setPropertyTypeId(UICommodityType.VMEM.typeNumber());
        result.setCapacity(Double.valueOf(HIST_UTILIZATION_CAPACITY));
        result.setPropertySlot(0);
        result.setUtilization(BigDecimal.valueOf(0));
        result.setValueType(HistoryUtilizationType.Timeslot.ordinal());
        multimap.put(TEST_RECORD_KEY, result);
        final VmStatsLatestRecord vmStatsLatestRecord = new VmStatsLatestRecord();
        vmStatsLatestRecord.setSnapshotTime(SNAPSHOT_TIME);
        vmStatsLatestRecord.setPropertyType(UICommodityType.VMEM.displayName());
        vmStatsLatestRecord.setCapacity(Double.valueOf(CAPACITY));
        multimap.put(TEST_RECORD_KEY, vmStatsLatestRecord);
        final SharedPropertyPopulator<Long> sharedPropertyPopulator =
                Mockito.mock(SharedPropertyPopulator.class);
        final Entry<Timestamp, Multimap<String, Record>> entry =
                new SimpleEntry<>(SNAPSHOT_TIME, multimap);
        final SnapshotCreator snapshotCreator = new SnapshotCreator(false, sharedPropertyPopulator);
        final StatSnapshot build = snapshotCreator.apply(entry).build();
        Assert.assertEquals(1, build.getStatRecordsCount());
        final StatRecord statRecord = build.getStatRecordsList().iterator().next();
        Assert.assertThat(HIST_UTILIZATION_CAPACITY, Matchers.is(
                statRecord.getHistUtilizationValueList()
                        .iterator()
                        .next()
                        .getCapacity()
                        .getTotal()));
        Assert.assertThat(CAPACITY, Matchers.is(statRecord.getCapacity().getTotal()));
    }

    @Test
    public void fullMarket_createStatSnapshots_expectRelatedEntityType() {
        // arrange
        final String entityType = "entityType";
        final String entityType2 = "entityType2";
        final List<Record> statsRecordsList = Lists.newArrayList(
            newMarketStatRecordWithEntityType(SNAPSHOT_TIME, 1.1d, C_1, C_1_SUBTYPE,
                COMMODITIES, entityType),
            newMarketStatRecordWithEntityType(SNAPSHOT_TIME, 1.2d, C_2, C_2_SUBTYPE,
                COMMODITIES, entityType2),
            newMarketStatRecordWithEntityType(SNAPSHOT_TIME, 1.3d, C_2, C_2_SUBTYPE,
                COMMODITIESBOUGHT, entityType2));

        final CommodityRequest commodityRequest = CommodityRequest.newBuilder()
            .setRelatedEntityType(entityType)
            .build();
        // act
        final List<StatSnapshot> snapshots =
            snapshotCreator.createStatSnapshots(statsRecordsList, true,
                Lists.newArrayList(commodityRequest))
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());
        // assert
        assertThat(snapshots.size(), equalTo(1));
        final StatSnapshot snapshot = snapshots.iterator().next();
        final List<StatRecord> statRecordsListReturned = snapshot.getStatRecordsList();
        assertThat(statRecordsListReturned.size(), equalTo(3));

        final List<StatRecord> expectedStatRecords = Lists.newArrayList(
            expectedStatRecord(C_1, 1.1f, COMMODITIES, entityType),
            expectedStatRecord(C_2, 1.2f, COMMODITIES, entityType2),
            expectedStatRecord(C_2, 1.3f, COMMODITIESBOUGHT, entityType2));

        assertThat(statRecordsListReturned.size(), equalTo(3));
        assertStringPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getName);
        assertStringPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getStatKey);
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getCurrentValue);
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            s -> s.getValues().getMin());
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            s -> s.getValues().getMax());
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            s -> s.getValues().getTotal());
        assertStringPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getRelatedEntityType);
        // more complicated comparisons for the StatValue fields
        assertStatValuesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getValues);
        assertStatValuesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getUsed);
        assertStatValuesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getPeak);
        assertStatValuesEqual(statRecordsListReturned, expectedStatRecords,
            StatRecord::getCapacity);
    }

    /**
     * Assert that all four fields - min, max, avg, total - of each returned statRecordList match
     * the corrsponding fields of the expected Stats Records. We need to compare the individual
     * fields because these are Floats and require a comparison epsilon.
     *
     * @param statRecordsListReturned StatRecords to be tested
     * @param expectedStatRecords StatRecord values we expect
     * @param getStatValueField function to pull one StatValue field from a StatRecord
     */
    private void assertStatValuesEqual(final List<StatRecord> statRecordsListReturned,
                                       final List<StatRecord> expectedStatRecords,
                                       final Function<StatRecord, StatValue> getStatValueField) {
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            getStatValueField.andThen(StatValue::getMin));
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            getStatValueField.andThen(StatValue::getMax));
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            getStatValueField.andThen(StatValue::getAvg));
        assertFloatPropertiesEqual(statRecordsListReturned, expectedStatRecords,
            getStatValueField.andThen(StatValue::getTotal));
    }

    /**
     * Assert that Float fields of a StatRecord match the expected values - without
     * regard to order.
     *
     * @param statRecordsListReturned StatRecords to be tested
     * @param expectedStatRecords StatRecords with the expected values
     * @param statAccessorFunction a Function to extract a float to be compared from
     *                             a given StatRecord
     */
    private void assertFloatPropertiesEqual(final List<StatRecord> statRecordsListReturned,
                                            final List<StatRecord> expectedStatRecords,
                                            final Function<StatRecord, Float> statAccessorFunction) {

        // sort both arrays because assertArrayEquals() respects order
        final double[] expectedArray = expectedStatRecords.stream()
            .map(statAccessorFunction)
            .mapToDouble(Float::doubleValue)
            .sorted()
            .toArray();
        final double[] actualArray = statRecordsListReturned.stream()
            .map(statAccessorFunction)
            .mapToDouble(Float::doubleValue)
            .sorted()
            .toArray();

        assertArrayEquals(expectedArray, actualArray, FLOAT_COMPARISON_EPSILON);
    }

    private void assertStringPropertiesEqual(final List<StatRecord> statRecordsListReturned,
                                             final List<StatRecord> expectedStatRecords,
                                             final Function<StatRecord, String> statAccessorFunction) {
        final List<String> expectedValues = expectedStatRecords.stream()
            .map(statAccessorFunction)
            .collect(Collectors.toList());

        assertThat(expectedValues, containsInAnyOrder(statRecordsListReturned.stream()
            .map(statAccessorFunction).toArray()));
    }
}
