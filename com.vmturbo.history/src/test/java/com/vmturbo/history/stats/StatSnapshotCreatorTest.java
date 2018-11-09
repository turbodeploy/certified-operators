package com.vmturbo.history.stats;

import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKey;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKeyAndEffectiveCapacity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Record;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;

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
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(), any(), any(), any(), any(), any(), any()))
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

    @Test
    public void testNoGroupByKey() {
        // arrange
        final StatRecord record = StatRecord.newBuilder()
                .setName("mock")
                .build();
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(), any(), any(), any(), any(), any(), any()))
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
