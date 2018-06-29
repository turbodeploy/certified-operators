package com.vmturbo.history.stats;

import static com.vmturbo.history.stats.StatsTestUtils.newStatRecordWithKey;
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

import org.jooq.Record;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.history.schema.StringConstants;
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
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(), any(), any(), any(), any(), any()))
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
        when(statRecordBuilder.buildStatRecord(any(), any(), isA(StatValue.class), any(), any(), any(), any(), any(), any(), any()))
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
}
