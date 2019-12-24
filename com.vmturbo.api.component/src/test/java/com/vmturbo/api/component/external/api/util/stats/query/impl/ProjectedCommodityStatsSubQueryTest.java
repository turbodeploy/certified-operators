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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ProjectedCommodityStatsSubQueryTest {

    private static final Duration LIVE_STATS_WINDOW = Duration.ofSeconds(1);

    private final StatsMapper statsMapper = mock(StatsMapper.class);

    private final StatsHistoryServiceMole backend = spy(StatsHistoryServiceMole.class);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);


    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(backend);

    private ProjectedCommodityStatsSubQuery query;

    @Before
    public void setup() {
        query = new ProjectedCommodityStatsSubQuery(LIVE_STATS_WINDOW, statsMapper,
            StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel()));
    }

    @Test
    public void testNotApplicableNoTimeWindow() {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getTimeWindow()).thenReturn(Optional.empty());

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableTimeWindow() {
        final TimeWindow timeWindow = ImmutableTimeWindow.builder()
            .startTime(1_000)
            // At the edge of the "cur stats window."
            .endTime(1_000 + LIVE_STATS_WINDOW.toMillis())
            .build();
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));
        when(context.getCurTime()).thenReturn(1_000L);

        // Not applicable because the time 2_000 still counts as "current", not "projected."
        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testNotApplicableToPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(context.requestProjected()).thenReturn(true);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testApplicableToNotPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);
        when(context.requestProjected()).thenReturn(true);
        when(context.getSessionContext()).thenReturn(userSessionContext);
        when(userSessionContext.isUserScoped()).thenReturn(false);
        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testNotApplicableWhenNoRequestProjected() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        // Request did not include projected stats.
        when(context.requestProjected()).thenReturn(false);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testGetAggregateStats() throws OperationFailedException {
        final TimeWindow timeWindow = ImmutableTimeWindow.builder()
            .startTime(1_000)
            .endTime(3_000)
            .build();
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));

        // Just something unique
        final StatSnapshot snapshot = StatSnapshot.newBuilder()
            .setSnapshotDate(1_000)
            .build();
        final ProjectedStatsResponse response = ProjectedStatsResponse.newBuilder()
            .setSnapshot(snapshot)
            .build();
        doReturn(response).when(backend).getProjectedStats(any());

        final StatSnapshotApiDTO mappedSnapshot = new StatSnapshotApiDTO();
        final StatApiDTO mappedStat = StatsTestUtil.stat("foo");
        mappedSnapshot.setStatistics(Collections.singletonList(mappedStat));

        when(statsMapper.toStatSnapshotApiDTO(snapshot)).thenReturn(mappedSnapshot);

        final List<StatSnapshotApiDTO> results = query.getAggregateStats(
            Collections.singleton(StatsTestUtil.statInput("foo")), context);

        // ASSERT
        final ArgumentCaptor<ProjectedStatsRequest> reqCaptor =
            ArgumentCaptor.forClass(ProjectedStatsRequest.class);
        verify(backend).getProjectedStats(reqCaptor.capture());
        final ProjectedStatsRequest req = reqCaptor.getValue();
        assertThat(req.getCommodityNameList(), containsInAnyOrder("foo"));
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(timeWindow.endTime(), DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        final List<StatApiDTO> stats = resultSnapshot.getStatistics();
        assertThat(stats.size(), is(1));
        assertThat(stats, is(mappedSnapshot.getStatistics()));
    }
}
