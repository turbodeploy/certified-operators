package com.vmturbo.extractor.action.commodity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ActionCommodity;

/**
 * Unit tests for {@link ProjectedTopologyCommodityDataRetriever}.
 */
public class ProjectedTopologyActionCommodityDataRetrieverTest {

    private StatsHistoryServiceMole statsHistoryServiceMole = spy(StatsHistoryServiceMole.class);

    /**
     * GRPC test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(statsHistoryServiceMole);

    private ProjectedTopologyCommodityDataRetriever projectedTopologyCommodityDataRetriever;

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        projectedTopologyCommodityDataRetriever = new ProjectedTopologyCommodityDataRetriever(
                StatsHistoryServiceGrpc.newBlockingStub(server.getChannel()));
    }

    /**
     * Test fetching the stats.
     */
    @Test
    public void testFetchPercentileData() {
        EntityStats e1Vmem = makeStat(1L, UICommodityType.VMEM, 10, 100);
        EntityStats e1Vcpu = makeStat(1L, UICommodityType.VCPU, 20, 100);
        EntityStats e2Vmem = makeStat(2L, UICommodityType.VMEM, 30, 100);
        setupStatsResults(Arrays.asList(e1Vmem, e1Vcpu, e2Vmem));

        final LongSet input = new LongOpenHashSet();
        input.add(1L);
        input.add(2L);

        final IntSet commInput = new IntOpenHashSet();
        commInput.add(UICommodityType.VMEM.typeNumber());
        commInput.add(UICommodityType.VCPU.typeNumber());

        final TopologyActionCommodityData projData =
                projectedTopologyCommodityDataRetriever.fetchProjectedCommodityData(input, commInput);
        ActionCommodity e1Comm = projData.getSoldCommms(1L).get((short)UICommodityType.VMEM.typeNumber());
        assertThat(e1Comm.getUsed(), is(10.0f));
        ActionCommodity e1VcpuComm = projData.getSoldCommms(1L).get((short)UICommodityType.VCPU.typeNumber());
        assertThat(e1VcpuComm.getUsed(), is(20.0f));
        ActionCommodity e3Comm = projData.getSoldCommms(2L).get((short)UICommodityType.VMEM.typeNumber());
        assertThat(e3Comm.getUsed(), is(30.0f));

        assertThat(projData.getSoldPercentile(1L, CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build()).get(), closeTo(10, 0.0001));
        assertThat(projData.getSoldPercentile(1L, CommodityType.newBuilder()
                .setType(UICommodityType.VCPU.typeNumber())
                .build()).get(), closeTo(20, 0.0001));
        assertThat(projData.getSoldPercentile(2L, CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build()).get(), closeTo(30, 0.0001));

    }

    private EntityStats makeStat(long entityId, UICommodityType commType, int usage, int capacity) {
        return EntityStats.newBuilder()
                .setOid(entityId)
                .addStatSnapshots(StatSnapshot.newBuilder()
                   .addStatRecords(StatRecord.newBuilder()
                       .setName(commType.apiStr())
                       .setUsed(StatValue.newBuilder()
                           .setTotal(usage))
                       .setCapacity(StatValue.newBuilder()
                               .setTotal(usage))
                       .addHistUtilizationValue(HistUtilizationValue.newBuilder()
                           .setType("percentile")
                           .setUsage(StatValue.newBuilder()
                               .setTotal(usage))
                           .setCapacity(StatValue.newBuilder()
                               .setTotal(capacity)))))
                .build();
    }

    /**
     * Set up the fake backend to return the stats, one per page.
     *
     * @param stats The stats.
     */
    private void setupStatsResults(List<EntityStats> stats) {
        doAnswer(invocationOnMock -> {
            ProjectedEntityStatsRequest req = invocationOnMock.getArgumentAt(0, ProjectedEntityStatsRequest.class);
            if (!req.getPaginationParams().hasCursor()) {
                return ProjectedEntityStatsResponse.newBuilder()
                        .addEntityStats(stats.get(0))
                        .setPaginationResponse(PaginationResponse.newBuilder()
                                .setNextCursor("1"))
                        .build();
            } else {
                int curIdx = Integer.parseInt(req.getPaginationParams().getCursor());
                int nextIdx = curIdx + 1;
                if (nextIdx == stats.size()) {
                    return ProjectedEntityStatsResponse.newBuilder()
                        .addEntityStats(stats.get(curIdx))
                        .build();
                } else {
                    return ProjectedEntityStatsResponse.newBuilder()
                        .addEntityStats(stats.get(curIdx))
                        .setPaginationResponse(PaginationResponse.newBuilder()
                            .setNextCursor(Integer.toString(nextIdx)))
                        .build();
                }
            }
        }).when(statsHistoryServiceMole).getProjectedEntityStats(any());
    }

}