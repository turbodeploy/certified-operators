package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class ClusterStatsSubQueryTest {

    private StatsHistoryServiceMole backend = spy(StatsHistoryServiceMole.class);

    private StatsMapper statsMapper = mock(StatsMapper.class);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);


    private ClusterStatsSubQuery query;

    @Before
    public void setup() {
        query = new ClusterStatsSubQuery(statsMapper,
            StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    @Test
    public void testApplicableToCluster() {
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.of(GroupType.COMPUTE_HOST_CLUSTER));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        StatApiInputDTO clusterStat = new StatApiInputDTO();
        clusterStat.setName(StringConstants.HEADROOM_VMS);
        when(context.getRequestedStats()).thenReturn(new HashSet<>(Arrays.asList(clusterStat)));
        assertThat(query.applicableInContext(context), is(true));
    }

    /**
     * Test that the query is cluster query, since it doesn't contain any cluster
     * stats in the request.
     */
    @Test
    public void testApplicableToClusterWithEmptyStats() {
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.of(GroupType.COMPUTE_HOST_CLUSTER));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(context.getRequestedStats()).thenReturn(new HashSet<>(new ArrayList<>()));
        assertThat(query.applicableInContext(context), is(true));
    }

    /**
     * Test that the query is not a cluster query, since it doesn't contain any cluster
     * commodities in the request.
     */
    @Test
    public void testNotApplicableToCluster() {
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.of(GroupType.COMPUTE_HOST_CLUSTER));
        StatApiInputDTO nonClusterStat = new StatApiInputDTO();
        nonClusterStat.setName(StringConstants.CPU);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(context.getRequestedStats()).thenReturn(new HashSet<>(Arrays.asList(nonClusterStat)));
        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableToGroup() {
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.of(GroupType.REGULAR));

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableToNonGroup() {
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.empty());

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final ApiId scope = mock(ApiId.class);
        when(scope.uuid()).thenReturn("1");

        final StatApiInputDTO apiInputDto = new StatApiInputDTO();

        final StatPeriodApiInputDTO periodInputDto = new StatPeriodApiInputDTO();
        periodInputDto.setStartDate("123");

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(context.newPeriodInputDto(any())).thenReturn(periodInputDto);

        final ClusterStatsRequest clusterStatsRequest = ClusterStatsRequest.newBuilder()
            .setClusterId(1231)
            .build();
        final StatSnapshot statSnapshot = StatSnapshot.newBuilder()
            .setSnapshotDate(1111111)
            .build();

        when(statsMapper.toClusterStatsRequest("1", periodInputDto)).thenReturn(clusterStatsRequest);
        doReturn(Collections.singletonList(statSnapshot)).when(backend).getClusterStats(any());

        final StatSnapshotApiDTO mappedApiSnapshot = new StatSnapshotApiDTO();
        mappedApiSnapshot.setDate(DateTimeUtil.toString(1_000_000));
        mappedApiSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("bar")));

        when(statsMapper.toStatSnapshotApiDTO(statSnapshot)).thenReturn(mappedApiSnapshot);

        // ACT
        final List<StatSnapshotApiDTO> results =
            query.getAggregateStats(Collections.singleton(apiInputDto), context);

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.iterator().next();
        assertThat(resultSnapshot.getStatistics(), is(mappedApiSnapshot.getStatistics()));
    }

}
