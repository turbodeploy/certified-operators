package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;

/**
 * Unit tests for TimeSlotLoadingTask.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({StatsHistoryServiceBlockingStub.class})
public class TimeSlotLoadingTaskTest {
    private static final TimeslotHistoricalEditorConfig CONFIG =
                    new TimeslotHistoricalEditorConfig(1, 1, 1, 1, 1, 1, Clock.systemUTC(),
                                    null);
    private static final long OID1 = 12;
    private static final long OID2 = 15;
    private static final CommodityType CT = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

    /**
     * Test the successful loading from history component.
     *
     * @throws HistoryCalculationException when failed
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testLoadSuccess() throws HistoryCalculationException, InterruptedException {
        StatsHistoryServiceBlockingStub history = PowerMockito.mock(StatsHistoryServiceBlockingStub.class);

        EntityCommodityReference ref1 = new EntityCommodityReference(OID1, CT, null);
        EntityCommodityReference ref2 = new EntityCommodityReference(OID2, CT, null);

        long t1 = 100;
        long t2 = 200;

        Answer<GetEntityStatsResponse> answerGetStats = new Answer<GetEntityStatsResponse>() {
            @Override
            public GetEntityStatsResponse answer(InvocationOnMock invocation) throws Throwable {
                GetEntityStatsRequest request = invocation
                                .getArgumentAt(0, GetEntityStatsRequest.class);
                Assert.assertNotNull(request);
                Assert.assertEquals(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS,
                                request.getFilter().getRollupPeriod());
                Assert.assertEquals(1, request.getFilter().getCommodityRequestsCount());
                Assert.assertEquals(UICommodityType.fromType(CT.getType()),
                                UICommodityType.fromString(request.getFilter()
                                                .getCommodityRequests(0).getCommodityName()));
                // two snapshots with one record per entity each
                return GetEntityStatsResponse.newBuilder()
                                .addEntityStats(createEntityStats(OID1, t1, t2))
                                .addEntityStats(createEntityStats(OID2, t1, t2))
                                .build();
            }
        };

        Mockito.doAnswer(answerGetStats).when(history).getEntityStats(Mockito.any());

        TimeSlotLoadingTask task = new TimeSlotLoadingTask(history, Pair.create(null, null));
        Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> comms = task
                        .load(ImmutableList.of(ref1, ref2), CONFIG);

        Assert.assertNotNull(comms);
        Assert.assertEquals(2, comms.size());
        List<Pair<Long, StatRecord>> ref1records = comms
                        .get(new EntityCommodityFieldReference(ref1, CommodityField.USED));
        Assert.assertNotNull(ref1records);
        Assert.assertEquals(2, ref1records.size());
        Assert.assertEquals(t1, ref1records.get(0).getFirst().longValue());
        Assert.assertEquals(t2, ref1records.get(1).getFirst().longValue());
    }

    private static EntityStats createEntityStats(long oid, long t1, long t2) {
        return EntityStats.newBuilder().setOid(oid).addStatSnapshots(createSnapshot(t1))
                        .addStatSnapshots(createSnapshot(t2)).build();
    }

    private static StatSnapshot createSnapshot(long time) {
        StatRecord record = StatRecord.newBuilder().setName(UICommodityType.POOL_CPU.apiStr()).build();
        return StatSnapshot.newBuilder().setSnapshotDate(time).addStatRecords(record).build();
    }
}
