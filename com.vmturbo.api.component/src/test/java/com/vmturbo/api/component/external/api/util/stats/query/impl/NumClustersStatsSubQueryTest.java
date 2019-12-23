package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class NumClustersStatsSubQueryTest {

    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceMole);

    private NumClustersStatsSubQuery query;

    @Before
    public void setup() {
        query = new NumClustersStatsSubQuery(GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    /**
     * Test that the query is applicable if the given stats is requested for global env.
     */
    @Test
    public void testApplicableInGlobalContextIfStatsRequested() {
        final StatApiInputDTO numClusters = StatsTestUtil.statInput(StringConstants.NUM_CLUSTERS);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(true);
        when(context.includeCurrent()).thenReturn(true);
        when(context.findStats(any())).thenReturn(Collections.singleton(numClusters));

        assertThat(query.applicableInContext(context), is(true));
    }

    /**
     * Test that the query is not applicable if the given stats is not requested for global env.
     */
    @Test
    public void testNotApplicableInGlobalContextIfStatsNotRequested() {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(true);
        when(context.includeCurrent()).thenReturn(true);
        when(context.findStats(any())).thenReturn(Collections.emptySet());

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testApplicableInNonGlobalContext() {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(false);
        when(context.includeCurrent()).thenReturn(true);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableIfNotIncludeCurrent() {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.isGlobalScope()).thenReturn(true);
        when(context.includeCurrent()).thenReturn(false);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testGetHandledStats() {
        final StatApiInputDTO numClusters = StatsTestUtil.statInput(StringConstants.NUM_CLUSTERS);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.includeCurrent()).thenReturn(true);
        when(context.findStats(any()))
            .thenReturn(Collections.singleton(numClusters));

        final SubQuerySupportedStats supportedStats = query.getHandledStats(context);

        verify(context).findStats(Collections.singleton(StringConstants.NUM_CLUSTERS));
        assertThat(supportedStats.supportsLeftovers(), is(false));
        assertThat(supportedStats.containsExplicitStat(numClusters), is(true));
    }

    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final float count = 10;
        final long curTime = 1_000_000;
        doReturn(CountGroupsResponse.newBuilder()
            .setCount((int)count)
            .build()).when(groupServiceMole).countGroups(any());

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getCurTime()).thenReturn(curTime);

        // ACT
        final Map<Long, List<StatApiDTO>> retStats = query.getAggregateStats(Collections.singleton(
            StatsTestUtil.statInput(StringConstants.NUM_CLUSTERS)), context);

        ArgumentCaptor<GetGroupsRequest> requestCaptor = ArgumentCaptor.forClass(GetGroupsRequest.class);
        verify(groupServiceMole).countGroups(requestCaptor.capture());

        GetGroupsRequest req = requestCaptor.getValue();
        assertEquals(GroupType.COMPUTE_HOST_CLUSTER, req.getGroupFilter().getGroupType());

        assertThat(retStats.keySet(), containsInAnyOrder(curTime));
        assertThat(retStats.get(curTime).size(), is(1));
        final StatApiDTO stat = retStats.get(curTime).get(0);
        assertThat(stat.getName(), is(StringConstants.NUM_CLUSTERS));
        assertThat(stat.getValue(), is(count));
        assertThat(stat.getValues().getTotal(), is(count));
        assertThat(stat.getValues().getMin(), is(count));
        assertThat(stat.getValues().getMax(), is(count));
        assertThat(stat.getValues().getAvg(), is(count));
    }
}