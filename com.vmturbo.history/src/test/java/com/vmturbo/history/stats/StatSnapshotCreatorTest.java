package com.vmturbo.history.stats;

import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKey;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKeyAndEffectiveCapacity;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithProducerUuid;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Record;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.live.LiveStatsReader;

public class StatSnapshotCreatorTest {

    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);

    final StatRecordBuilder statRecordBuilder = mock(StatRecordBuilder.class);

    final StatSnapshotCreator snapshotCreator = new DefaultStatSnapshotCreator(statRecordBuilder);

    @Test
    public void testGroupByKey() {
        final StatRecord record1 = StatRecord.newBuilder()
                .setName("mock1")
                .build();
        final StatRecord record2 = StatRecord.newBuilder()
                .setName("mock2")
                .build();
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(),
            any(), any(), any(), any(), any(), any()))
                .thenReturn(record1, record2);
        final List<CommodityRequest> commodityRequests =
                Collections.singletonList(CommodityRequest.newBuilder()
                        .setCommodityName("c1")
                        .addGroupBy(StringConstants.KEY)
                        .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
                newStatRecordWithKey(SNAPSHOT_TIME, 1, "c1", "c1-subtype", "key1"),
                newStatRecordWithKey(SNAPSHOT_TIME, 2, "c1", "c1-subtype", "key2"));

        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        assertThat(snapshot.getStatRecordsList(), containsInAnyOrder(record1, record2));
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

        LiveStatsReader liveStatsReader = mock(LiveStatsReader.class);
        when(liveStatsReader.getEntityDisplayNameForId(any())).thenReturn("dummyDisplayName");
        DefaultStatRecordBuilder statRecordBuilder1 = new DefaultStatRecordBuilder(liveStatsReader);
        final StatSnapshotCreator snapshotCreator1 = new DefaultStatSnapshotCreator(statRecordBuilder1);

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
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 12, storageAmount, "used", oidDiskArray1),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 13, storageAccess, "used", oidDiskArray1),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 14, storageAmount, "used", oidDiskArray2),
            newStatRecordWithProducerUuid(SNAPSHOT_TIME, 15, storageAccess, "used", oidDiskArray2)
        );

        final List<StatSnapshot> snapshots =
            snapshotCreator1.createStatSnapshots(statsRecordsList, false, commodityRequests)
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

        LiveStatsReader liveStatsReader = mock(LiveStatsReader.class);
        when(liveStatsReader.getEntityDisplayNameForId(any())).thenReturn("dummyDisplayName");
        DefaultStatRecordBuilder statRecordBuilder1 = new DefaultStatRecordBuilder(liveStatsReader);
        final StatSnapshotCreator snapshotCreator1 = new DefaultStatSnapshotCreator(statRecordBuilder1);

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
            newStatRecordWithKey(SNAPSHOT_TIME, 12, storageAmount, "used", volume1),
            newStatRecordWithKey(SNAPSHOT_TIME, 13, storageAccess, "used", volume1),
            newStatRecordWithKey(SNAPSHOT_TIME, 14, storageAmount, "used", volume2),
            newStatRecordWithKey(SNAPSHOT_TIME, 15, storageAccess, "used", volume2)
        );

        final List<StatSnapshot> snapshots =
            snapshotCreator1.createStatSnapshots(statsRecordsList, false, commodityRequests)
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
        final StatRecord record = StatRecord.newBuilder()
                .setName("mock")
                .build();
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(),
            any(), any(), any(), any(), any(), any()))
                .thenReturn(record);
        final List<CommodityRequest> commodityRequests =
            Collections.singletonList(CommodityRequest.newBuilder()
                .setCommodityName("c1")
                .build());

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecordWithKey(SNAPSHOT_TIME, 1, "c1", "c1-subtype", "key1"),
            newStatRecordWithKey(SNAPSHOT_TIME, 2, "c1", "c1-subtype", "key2"));

        final List<StatSnapshot> snapshots =
            snapshotCreator.createStatSnapshots(statsRecordsList, false, commodityRequests)
                .map(StatSnapshot.Builder::build)
                .collect(Collectors.toList());

        assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        assertThat(snapshot.getStatRecordsList(), containsInAnyOrder(record));
    }

    @Test
    public void testEffectiveCapacityCalculation() {
        // verify that the effective capacity calculation in the stat snapshot is behaving as expected
        // we need a non-mocked stat record builder to do this.
        final StatSnapshotCreator snapshotCreator = new DefaultStatSnapshotCreator(new TestStatRecordBuilder());

        final List<Record> statsRecordsList = Lists.newArrayList(
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 1, 1.0, "c1", "c1-subtype", "key1"),
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 1, 0.5, "c1", "c1-subtype", "key1"),
                newStatRecordWithKeyAndEffectiveCapacity(SNAPSHOT_TIME, 2, 0.5,"c1", "c1-subtype", "key1"));

        final List<StatSnapshot> snapshots =
                snapshotCreator.createStatSnapshots(statsRecordsList, false, Collections.emptyList())
                        .map(StatSnapshot.Builder::build)
                        .collect(Collectors.toList());

        Assert.assertEquals(1, snapshots.size());
        // average capacity should be (3+3+6)/3 = 4, and average effective capacity should be (3+1.5+3)/3 = 2.5
        // so we expect the "reserved" amount to be (capacity - effective capacity) = (4 - 2.5) = 1.5
        Assert.assertEquals(1.5, snapshots.get(0).getStatRecords(0).getReserved(), 0);
    }

    class TestStatRecordBuilder implements StatRecordBuilder {

        @Nonnull
        @Override
        public StatRecord buildStatRecord(@Nonnull final String propertyType, @Nullable final String propertySubtype, @Nullable final StatValue capacityStat, @Nullable final Float reserved, @Nullable final Long producerId, @Nullable final Float avgValue, @Nullable final Float minValue, @Nullable final Float maxValue, @Nullable final String commodityKey, @Nullable final Float totalValue, @Nullable final String relation) {
            final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                    .setName(propertyType);

            if (capacityStat != null) {
                statRecordBuilder.setCapacity(capacityStat);
            }

            if (relation != null) {
                statRecordBuilder.setRelation(relation);
            }

            if (reserved != null) {
                statRecordBuilder.setReserved(reserved);
            }

            if (commodityKey != null) {
                statRecordBuilder.setStatKey(commodityKey);
            }
            if (producerId != null) {
                // providerUuid
                statRecordBuilder.setProviderUuid(Long.toString(producerId));
                statRecordBuilder.setProviderDisplayName(statRecordBuilder.getProviderUuid());
            }

            // units
            CommodityTypeUnits commodityType = CommodityTypeUnits.fromString(propertyType);
            if (commodityType != null) {
                statRecordBuilder.setUnits(commodityType.getUnits());
            }

            // values, used, peak
            StatValue.Builder statValueBuilder = StatValue.newBuilder();
            if (avgValue != null) {
                statValueBuilder.setAvg(avgValue);
            }
            if (minValue != null) {
                statValueBuilder.setMin(minValue);
            }
            if (maxValue != null) {
                statValueBuilder.setMax(maxValue);
            }
            if (totalValue != null) {
                statValueBuilder.setTotal(totalValue);
            }

            // currentValue
            if (avgValue != null && (propertySubtype == null ||
                    StringConstants.PROPERTY_SUBTYPE_USED.equals(propertySubtype))) {
                statRecordBuilder.setCurrentValue(avgValue);
            } else {
                if (maxValue != null) {
                    statRecordBuilder.setCurrentValue(maxValue);
                }
            }

            StatValue statValue = statValueBuilder.build();

            statRecordBuilder.setValues(statValue);
            statRecordBuilder.setUsed(statValue);
            statRecordBuilder.setPeak(statValue);

            return statRecordBuilder.build();
        }
    }
}
