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

import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
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

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.HistoryUtilizationType;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.BuStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.CntStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.VmStatsLatestRecord;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProducerIdPopulator;
import com.vmturbo.history.stats.snapshots.PropertyTypeVisitor;
import com.vmturbo.history.stats.snapshots.SnapshotCreator;
import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;
import com.vmturbo.platform.common.dto.CommonDTO;

public class StatSnapshotCreatorTest {

    private static final float CAPACITY = 1000F;
    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);
    private static final String C_1 = "c1";
    private static final String C_2 = "c2";
    private static final String C_1_SUBTYPE = PropertySubType.Utilization.getApiParameterName();
    private static final String C_2_SUBTYPE = PropertySubType.Utilization.getApiParameterName();
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String USED = PropertySubType.Used.getApiParameterName();
    private static final float FLOAT_COMPARISON_EPSILON = 0.001F;
    private static final long PRODUCER_OID = 111L;
    private static final String PROVIDER_DISPLAY_NAME = "providerDisplayName";
    private static final String COMMODITY_KEY = "commodityKey";
    private static final String TEST_RECORD_KEY = "testRecord";
    private static final float HIST_UTILIZATION_CAPACITY = 1500F;
    private static final Double MIN_USED = 300D;
    private static final Double AVG_USED = 500D;
    private static final Double MAX_USED = 700D;
    private static final Double DELTA = 0.0001D;

    private final LiveStatsReader liveStatsReader = Mockito.mock(LiveStatsReader.class);
    private final StatSnapshotCreator snapshotCreator = StatsConfig.createStatSnapshotCreator(liveStatsReader);

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
     * Test group by relatedEntity for StatSnapshotCreator.
     *
     * <p>It tests the scenario that one LogicalPool consumes two different disk arrays. We expect
     * to see two set of bought commodities in the result.</p>
     *
     * <p>UI usually passes commodity request filters with all groupBy fields:</p>
     * <dl>
     *     <dt>commodityName</dt>
     *     <dd>StorageAmount, groupBy: [key, relatedEntity, virtualDisk]</dd>
     *     <dt>commodityName</dt>
     *     <dd>StorageAccess, groupBy: [key, relatedEntity, virtualDisk]</dd>
     * </dl>
     *
     * <p>Only relatedEntity has effect here, but the others are also added in this test to ensure
     * they don't have effect on the result. This test will verify that there are two StorageAmount
     * and two StorageAccess (related to different provider) with different used value in result.</p>
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
     * Test group by virtualDisk for StatSnapshotCreator.
     *
     * <p>It tests the scenario that one VM buys
     * two set of commodities from same StorageTier (each set is related to a different volume).
     * Volume id is stored in the commodity key field. We expect to see two set of bought
     * commodities with different keys in the result.</p>
     *
     * <p>UI usually passes commodity request filters with all groupBy fields:</p>
     * <dl>
     *     <dt>commodityName</dt>
     *     <dd>StorageAmount, groupBy: [key, relatedEntity, virtualDisk]</dd>
     *     <dt>commodityName</dt>
     *     <dd>StorageAccess, groupBy: [key, relatedEntity, virtualDisk]</dd>
     * </dl>
     *
     * <p>Only virtualDisk has effect here, but the others are also added in this test to ensure
     * they don't have effect on the result. This test will verify that there are two StorageAmount
     * and two StorageAccess (related to different volume) with different used value in result.</p>
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

    /**
     * Test {@link SnapshotCreator} when no group-by is given in the request
     * and several DB records are returned.  The output should be a single
     * stat snapshot, with the aggregated values, and with "multiple providers"
     * as provider information.
     */
    @Test
    public void testNoGroupBy() {
        // arrange
        final List<CommodityRequest> commodityRequests =
                Collections.singletonList(CommodityRequest.newBuilder()
                        .setCommodityName(C_1)
                        .build());
        final long providerId1 = 100;
        final long providerId2 = 200;
        final String provider1 = "provider1";
        final String provider2 = "provider2";
        final String key1 = "key1";
        final String key2 = "key2";
        final float smallValue = 15.0f;
        final float bigValue = 20.0f;

        final List<Record> statsRecordsList = Lists.newArrayList(
                newStatRecordWithProducerUuid(
                        SNAPSHOT_TIME, smallValue, C_1, USED, Long.toString(providerId1), key1),
                newStatRecordWithProducerUuid(
                        SNAPSHOT_TIME, bigValue, C_1, USED, Long.toString(providerId2), key2));
        when(liveStatsReader.getEntityDisplayNameForId(providerId1)).thenReturn(provider1);
        when(liveStatsReader.getEntityDisplayNameForId(providerId2)).thenReturn(provider2);

        // act
        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        // assert
        Assert.assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        Assert.assertThat(snapshot.getStatRecordsList().size(), is(1));

        final StatRecord record = snapshot.getStatRecordsList().iterator().next();
        Assert.assertTrue(record.hasProviderDisplayName());
        Assert.assertEquals(ProducerIdVisitor.MULTIPLE_PROVIDERS, record.getProviderDisplayName());
        Assert.assertFalse(record.hasProviderUuid());
        Assert.assertEquals(PropertyTypeVisitor.MULTIPLE_KEYS, record.getStatKey());

        final StatValue statValue = record.getUsed();
        Assert.assertEquals(bigValue * 2, statValue.getMax(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals(smallValue / 2, statValue.getMin(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals((bigValue + smallValue) / 2, statValue.getAvg(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals(bigValue + smallValue, statValue.getTotal(), FLOAT_COMPARISON_EPSILON);
    }

    /**
     * Test {@link SnapshotCreator} when no group-by is given in the request
     * and only one DB records is returned.  The output should be a single
     * stat snapshot and there should be provider information.
     */
    @Test
    public void testNoGroupByOneRecordOnly() {
        // arrange
        final List<CommodityRequest> commodityRequests =
                Collections.singletonList(CommodityRequest.newBuilder()
                        .setCommodityName(C_1)
                        .build());
        final long providerId = 100;
        final String provider = "provider";
        final String key = "key";
        final float value = 20.0f;

        final List<Record> statsRecordsList =
                Collections.singletonList(newStatRecordWithProducerUuid(
                        SNAPSHOT_TIME, value, C_1, USED, Long.toString(providerId), key));
        when(liveStatsReader.getEntityDisplayNameForId(providerId)).thenReturn(provider);

        // act
        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        // assert
        Assert.assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        Assert.assertThat(snapshot.getStatRecordsList().size(), is(1));

        final StatRecord record = snapshot.getStatRecordsList().iterator().next();
        Assert.assertTrue(record.hasProviderDisplayName());
        Assert.assertEquals(provider, record.getProviderDisplayName());
        Assert.assertTrue(record.hasProviderUuid());
        Assert.assertEquals(Long.toString(providerId), record.getProviderUuid());
        Assert.assertEquals(key, record.getStatKey());

        final StatValue statValue = record.getUsed();
        Assert.assertEquals(value * 2, statValue.getMax(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals(value / 2, statValue.getMin(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals(value, statValue.getAvg(), FLOAT_COMPARISON_EPSILON);
        Assert.assertEquals(value, statValue.getTotal(), FLOAT_COMPARISON_EPSILON);
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
        percentileRecord.setUtilization(0.4D);
        percentileRecord.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        percentileRecord.setCapacity(capacity);
        percentileRecord.setCommodityKey("percentileCommodityKey");
        percentileRecord.setPropertySlot(0);
        percentileRecord.setValueType(HistoryUtilizationType.Percentile.ordinal());
        percentileRecord.setProducerOid(1L);

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
     * Test commodity percentile utilization for commodity sold.
     */
    @Test
    public void testPercentileUtilizationCommoditySold() {
        final String userRecordCommodityKey = "userRecordCommodityKey";
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
        usedRecord.setRelation(COMMODITIES);
        usedRecord.setCommodityKey(userRecordCommodityKey);
        usedRecord.setAvgValue(AVG_USED);
        usedRecord.setMinValue(MIN_USED);
        usedRecord.setMaxValue(MAX_USED);
        final double capacity = new Double(CAPACITY);
        usedRecord.setCapacity(capacity);

        final HistUtilizationRecord percentileRecord = new HistUtilizationRecord();
        percentileRecord.setPropertyTypeId(UICommodityType.IMAGE_CPU.typeNumber());
        percentileRecord.setUtilization(0.4D);
        percentileRecord.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        percentileRecord.setCapacity(capacity);
        percentileRecord.setCommodityKey("percentileCommodityKey");
        percentileRecord.setPropertySlot(0);
        percentileRecord.setValueType(HistoryUtilizationType.Percentile.ordinal());
        percentileRecord.setProducerOid(0L);

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
        Assert.assertEquals(AVG_USED, statRecord.getUsed().getAvg(), DELTA);
        final HistUtilizationValue percentileValue =
            statRecord.getHistUtilizationValueList().stream()
                .filter(value -> HistoryUtilizationType.Percentile
                    .getApiParameterName()
                    .equals(value.getType()))
                .findAny().get();
        Assert.assertEquals(400D, percentileValue.getUsage().getAvg(), DELTA);
        Assert.assertEquals(capacity, percentileValue.getCapacity().getAvg(), DELTA);
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
        histUtilizationRecord.setUtilization(0.4D);
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
                        CoreMatchers.is(CommodityTypeMapping.getUnitForCommodityType(CommonDTO.CommodityDTO.CommodityType.IMAGE_CPU)));

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
        result.setUtilization(utilization);
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
     * Checks that capacity is accumulated per provider and not per consumer.
     */
    @Test
    public void checkCapacitiesForConsumerGroupWithMoreThanOneProvider() {
        // verify that the effective capacity calculation in the stat snapshot is behaving as expected
        // we need a non-mocked stat record builder to do this.
        final Collection<String> firstProviderConsumerUuids =
                        Arrays.asList("111", "222", "333", "444");
        final Collection<String> secondProviderConsumerUuids = Arrays.asList("555", "666", "777");
        final List<Record> records = new ArrayList<>(firstProviderConsumerUuids.size()
                        + secondProviderConsumerUuids.size());
        final String producer1Uuid = Long.toString(1000);

        firstProviderConsumerUuids.forEach(uuid -> records.add(StatsTestUtils
                        .createRecord(uuid, SNAPSHOT_TIME, C_1, C_1_SUBTYPE, 1D,
                                        producer1Uuid)));
        final String producer2Uuid = Long.toString(2000);
        secondProviderConsumerUuids.forEach(uuid -> records.add(StatsTestUtils
                        .createRecord(uuid, SNAPSHOT_TIME, C_1, C_1_SUBTYPE, 1D,
                                        producer2Uuid)));
        final Collection<StatSnapshot> snapshots =
                        snapshotCreator.createStatSnapshots(records, false, Collections.emptyList())
                                        .map(StatSnapshot.Builder::build)
                                        .collect(Collectors.toList());
        Assert.assertEquals(1, snapshots.size());
        final StatSnapshot snapshot = snapshots.iterator().next();
        Assert.assertThat(snapshot.getStatRecordsCount(), CoreMatchers.is(1));
        /*
         6 is expected number because:
         we had 7 VMs connected to two providers. For every VM we are creating test record with
         capacity equal to: testValue * 3 (please, refer to
         com.vmturbo.history.stats.StatsTestUtils.createRecord and
         com.vmturbo.history.stats.StatsTestUtils.testCapacity)
         So in total we will have 7 records: 4 records connected to one provider, 3 records
         connected to second provider. Every record has capacity equal to 3.
         As a result before the fix we would have 7 * 3 = 21. After the fix we are grouping capacity
         value by providers, i.e. will will have capacity equal to 3 from one and 3 from second, so
         total capacity value will be 6.
         */
        checkStatRecord(snapshot.getStatRecords(0), 6F, "",
                        ProducerIdVisitor.MULTIPLE_PROVIDERS);
    }

    /**
     * Checks that capacity accumulated per provider and not per consumer.
     */
    @Test
    public void checkCapacitiesForConsumerGroupWithMoreThanOneProviderGroupedByProvider() {
        final long provider1Oid = 1000;
        final long provider2Oid = 2000;
        final String provider1000DisplayName = "provider1000DisplayName";
        Mockito.when(liveStatsReader.getEntityDisplayNameForId(provider1Oid))
                        .thenReturn(provider1000DisplayName);
        final String provider2000DisplayName = "provider2000DisplayName";
        Mockito.when(liveStatsReader.getEntityDisplayNameForId(provider2Oid))
                        .thenReturn(provider2000DisplayName);
        // verify that the effective capacity calculation in the stat snapshot is behaving as expected
        // we need a non-mocked stat record builder to do this.
        final Collection<String> firstProviderConsumerUuids =
                        Arrays.asList("111", "222", "333", "444");
        final Collection<String> secondProviderConsumerUuids = Arrays.asList("555", "666", "777");
        final List<Record> records = new ArrayList<>(firstProviderConsumerUuids.size()
                        + secondProviderConsumerUuids.size());
        final String producer1Uuid = Long.toString(provider1Oid);
        firstProviderConsumerUuids.forEach(uuid -> records.add(StatsTestUtils
                        .createRecord(uuid, SNAPSHOT_TIME, C_1, C_1_SUBTYPE, 1D, producer1Uuid)));

        final String producer2Uuid = Long.toString(provider2Oid);
        secondProviderConsumerUuids.forEach(uuid -> records.add(StatsTestUtils
                        .createRecord(uuid, SNAPSHOT_TIME, C_1, C_1_SUBTYPE, 1D, producer2Uuid)));
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
                        CommodityRequest.newBuilder().setCommodityName(C_1)
                                        .addGroupBy(StringConstants.RELATED_ENTITY).build());
        final Collection<StatSnapshot> snapshots =
                        snapshotCreator.createStatSnapshots(records, false, commodityRequests)
                                        .map(StatSnapshot.Builder::build)
                                        .collect(Collectors.toList());
        Assert.assertEquals(1, snapshots.size());
        final StatSnapshot snapshot = snapshots.iterator().next();
        Assert.assertThat(snapshot.getStatRecordsCount(), CoreMatchers.is(2));
        checkStatRecord(snapshot.getStatRecords(0), 9F, producer2Uuid,
                        provider2000DisplayName);
        checkStatRecord(snapshot.getStatRecords(1), 12F, producer1Uuid,
                        provider1000DisplayName);
    }

    private static void checkStatRecord(StatRecord statRecord, float total, String providerUuid,
                    String expectedDisplayName) {
        Assert.assertThat(statRecord.getReserved(), CoreMatchers.is(0F));
        Assert.assertThat(statRecord.getCapacity().getTotal(), CoreMatchers.is(total));
        Assert.assertThat(statRecord.getProviderUuid(), CoreMatchers.is(providerUuid));
        Assert.assertThat(statRecord.getProviderDisplayName(),
                        CoreMatchers.is(expectedDisplayName));
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
        result.setUtilization(0.0);
        result.setValueType(HistoryUtilizationType.Timeslot.ordinal());
        multimap.put(TEST_RECORD_KEY, result);
        final VmStatsLatestRecord vmStatsLatestRecord = new VmStatsLatestRecord();
        vmStatsLatestRecord.setSnapshotTime(SNAPSHOT_TIME);
        vmStatsLatestRecord.setPropertyType(UICommodityType.VMEM.displayName());
        vmStatsLatestRecord.setPropertySubtype(PropertySubType.Used.getApiParameterName());
        vmStatsLatestRecord.setCapacity(Double.valueOf(CAPACITY));
        multimap.put(TEST_RECORD_KEY, vmStatsLatestRecord);
        final ProducerIdPopulator sharedPropertyPopulator = Mockito.mock(ProducerIdPopulator.class);
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

    /**
     * Test commodity unit of container VCPU stat record.
     */
    @Test
    public void testCommodityUnitOfContainerVCPUStatRecord() {
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
            CommodityRequest.newBuilder().setCommodityName(StringConstants.VCPU).build());
        final CntStatsLatestRecord containerRecord = new CntStatsLatestRecord();
        containerRecord.setSnapshotTime(SNAPSHOT_TIME);
        containerRecord.setPropertyType(StringConstants.VCPU);

        Collection<StatSnapshot> statSnapshots =
            snapshotCreator.createStatSnapshots(Collections.singletonList(containerRecord),
                false, commodityRequests)
            .map(StatSnapshot.Builder::build)
            .collect(Collectors.toList());
        assertEquals(1, statSnapshots.size());
        StatSnapshot statSnapshot = statSnapshots.iterator().next();
        assertEquals(1, statSnapshot.getStatRecordsCount());
        assertEquals("mCores", statSnapshot.getStatRecords(0).getUnits());
    }

    /**
     * Test commodity unit of container VCPU stat record with histUtilizationRecord.
     */
    @Test
    public void testCommodityUnitOfContainerVCPUStatWithHisUtilizationRecord() {
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
            CommodityRequest.newBuilder().setCommodityName(StringConstants.VCPU).build());
        final CntStatsLatestRecord containerStatsRecord = new CntStatsLatestRecord();
        containerStatsRecord.setSnapshotTime(SNAPSHOT_TIME);
        containerStatsRecord.setPropertyType(StringConstants.VCPU);
        containerStatsRecord.setPropertySubtype(USED);
        containerStatsRecord.setRelation(COMMODITIES);

        final HistUtilizationRecord percentileRecord = new HistUtilizationRecord();
        percentileRecord.setPropertyTypeId(UICommodityType.VCPU.typeNumber());
        percentileRecord.setUtilization(0.4D);
        percentileRecord.setPropertySubtypeId(PropertySubType.Utilization.ordinal());
        percentileRecord.setCapacity(1000D);
        percentileRecord.setCommodityKey("percentileCommodityKey");
        percentileRecord.setPropertySlot(0);
        percentileRecord.setValueType(HistoryUtilizationType.Percentile.ordinal());
        percentileRecord.setProducerOid(0L);

        Collection<StatSnapshot> statSnapshots =
            snapshotCreator.createStatSnapshots(Arrays.asList(containerStatsRecord, percentileRecord),
                false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());
        assertEquals(1, statSnapshots.size());
        StatSnapshot statSnapshot = statSnapshots.iterator().next();
        assertEquals(1, statSnapshot.getStatRecordsCount());
        StatRecord statRecord = statSnapshot.getStatRecords(0);
        // StatRecord has HistUtilization value.
        assertEquals(1, statRecord.getHistUtilizationValueCount());
        // VCPU unit of this container StatRecord is still "mCores".
        assertEquals("mCores", statRecord.getUnits());
    }

    /**
     * Test commodity unit of VM VCPU stat record.
     */
    @Test
    public void testCommodityUnitOfVMVCPUStatRecord() {
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
            CommodityRequest.newBuilder().setCommodityName(StringConstants.VCPU).build());
        final VmStatsLatestRecord vmRecord = new VmStatsLatestRecord();
        vmRecord.setSnapshotTime(SNAPSHOT_TIME);
        vmRecord.setPropertyType(StringConstants.VCPU);

        final Collection<StatSnapshot> statSnapshots =
            snapshotCreator.createStatSnapshots(Collections.singletonList(vmRecord),
                false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());
        assertEquals(1, statSnapshots.size());
        StatSnapshot statSnapshot = statSnapshots.iterator().next();
        assertEquals(1, statSnapshot.getStatRecordsCount());
        assertEquals("MHz", statSnapshot.getStatRecords(0).getUnits());
    }

    /**
     * Test commodity units from full market stats record with VM and container VCPU statRecords.
     */
    @Test
    public void testCommodityUnitsFromFullMarketStats() {
        final List<CommodityRequest> commodityRequests = Collections.singletonList(
            CommodityRequest.newBuilder().setCommodityName(StringConstants.VCPU)
                .addGroupBy(StringConstants.RELATED_ENTITY).build());

        final MarketStatsLatestRecord cntMarketStatsLatestRecord = new MarketStatsLatestRecord();
        cntMarketStatsLatestRecord.setSnapshotTime(SNAPSHOT_TIME);
        cntMarketStatsLatestRecord.setEntityType(StringConstants.VIRTUAL_MACHINE);
        cntMarketStatsLatestRecord.setPropertyType(StringConstants.VCPU);
        cntMarketStatsLatestRecord.setRelation(COMMODITIES);

        final MarketStatsLatestRecord vmMarketStatsLatestRecord = new MarketStatsLatestRecord();
        vmMarketStatsLatestRecord.setSnapshotTime(SNAPSHOT_TIME);
        vmMarketStatsLatestRecord.setEntityType(StringConstants.CONTAINER);
        vmMarketStatsLatestRecord.setPropertyType(StringConstants.VCPU);
        cntMarketStatsLatestRecord.setRelation(COMMODITIES);

        final Collection<StatSnapshot> statSnapshots =
            snapshotCreator.createStatSnapshots(Arrays.asList(cntMarketStatsLatestRecord, vmMarketStatsLatestRecord),
                true, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());
        assertEquals(1, statSnapshots.size());
        StatSnapshot statSnapshot = statSnapshots.iterator().next();
        assertEquals(2, statSnapshot.getStatRecordsCount());
        final List<StatRecord> expectedStatRecords = Lists.newArrayList(
            expectedStatRecord(StringConstants.VCPU, 1f, COMMODITIES, StringConstants.VIRTUAL_MACHINE, "MHz"),
            expectedStatRecord(StringConstants.VCPU, 1f, COMMODITIES, StringConstants.CONTAINER, "mCores"));
        assertStringPropertiesEqual(statSnapshot.getStatRecordsList(), expectedStatRecords,
            StatRecord::getRelatedEntityType);
        assertStringPropertiesEqual(statSnapshot.getStatRecordsList(), expectedStatRecords,
            StatRecord::getUnits);
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
