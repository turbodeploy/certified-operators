package com.vmturbo.api.component.external.api.util.stats.query.impl;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.ImmutableGlobalScope;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;

public class HistoricalCommodityStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;
    private static final StatsFilter FILTER = StatsFilter.newBuilder()
        // For uniqueness/equality comparison
        .setStartDate(1L)
        .build();

    private static final StatSnapshot HISTORY_STAT_SNAPSHOT = StatSnapshot.newBuilder()
        // For uniqueness/equality comparison.
        .setSnapshotDate(MILLIS)
        .build();

    private static final StatSnapshotApiDTO MAPPED_STAT_SNAPSHOT;

    static {
        MAPPED_STAT_SNAPSHOT = new StatSnapshotApiDTO();
        MAPPED_STAT_SNAPSHOT.setDate(DateTimeUtil.toString(MILLIS));
        MAPPED_STAT_SNAPSHOT.setEpoch(Epoch.HISTORICAL);
        MAPPED_STAT_SNAPSHOT.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));
    }

    private static final Set<StatApiInputDTO> REQ_STATS =
        Collections.singleton(StatsTestUtil.statInput("foo"));

    private final StatsMapper statsMapper = mock(StatsMapper.class);

    private final StatsHistoryServiceMole backend = spy(StatsHistoryServiceMole.class);

    @Captor
    public ArgumentCaptor<GetAveragedEntityStatsRequest> reqCaptor;

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private HistoricalCommodityStatsSubQuery query;

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final ApiId vmGroupScope = mock(ApiId.class);

    private final StatsQueryContext context = mock(StatsQueryContext.class);

    private static final StatPeriodApiInputDTO NEW_PERIOD_INPUT_DTO = new StatPeriodApiInputDTO();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new HistoricalCommodityStatsSubQuery(statsMapper,
            StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            userSessionContext);

        when(context.getInputScope()).thenReturn(vmGroupScope);

        when(statsMapper.newPeriodStatsFilter(any())).thenReturn(FILTER);

        when(statsMapper.toStatSnapshotApiDTO(HISTORY_STAT_SNAPSHOT)).thenReturn(MAPPED_STAT_SNAPSHOT);

        doReturn(Collections.singletonList(HISTORY_STAT_SNAPSHOT))
            .when(backend).getAveragedEntityStats(any());

        when(context.newPeriodInputDto(any())).thenReturn(NEW_PERIOD_INPUT_DTO);

        when(vmGroupScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE)));
    }

    @Test
    public void testNotApplicableInPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testApplicableInNotPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testStatsRequest() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // Not a global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(false);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testGlobalGroupStatsRequest() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // A global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(true);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(ApiEntityType.VIRTUAL_MACHINE)
            .environmentType(EnvironmentType.CLOUD)
            .build()));
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // normalize vm to vm
        when(statsMapper.normalizeRelatedType(ApiEntityType.VIRTUAL_MACHINE.apiStr())).thenReturn(
            ApiEntityType.VIRTUAL_MACHINE.apiStr());

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        // We should pass the global type to the stats mapper.
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        // No entities, because it's a global temp group.
        assertThat(req.getEntitiesList(), is(Collections.emptyList()));
        // The type of the scope group.
        assertThat(req.getGlobalFilter().getRelatedEntityTypeList(), containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(req.getGlobalFilter().getEnvironmentType(), is(EnvironmentType.CLOUD));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testScopedGlobalGroupStatsRequest() throws OperationFailedException {
        // Scoped user
        when(userSessionContext.isUserScoped()).thenReturn(true);

        // A global temp group (for the user scope).
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(true);

        // Don't include current stats.
        when(context.includeCurrent()).thenReturn(false);

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        // Shouldn't treat it as a GLOBAL temp group.
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        // Shouldn't treat it as a GLOBAL temp group.
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        assertEquals(1, results.size());
        final StatSnapshotApiDTO resultSnapshot = results.get(0);
        assertEquals(MILLIS, DateTimeUtil.parseTime(resultSnapshot.getDate()).longValue());
        assertThat(resultSnapshot.getStatistics(), is(MAPPED_STAT_SNAPSHOT.getStatistics()));
        assertEquals(MAPPED_STAT_SNAPSHOT, resultSnapshot);
    }

    @Test
    public void testIncludeCurrentCopyLast() throws OperationFailedException {
        // Non-scoped user.
        when(userSessionContext.isUserScoped()).thenReturn(false);

        // Not a global temp group.
        when(vmGroupScope.isGlobalTempGroup()).thenReturn(false);

        final long startTime = MILLIS;
        final long currentTime = MILLIS * 2;

        // Include current stats.
        when(context.includeCurrent()).thenReturn(true);
        when(context.getCurTime()).thenReturn(currentTime);
        when(context.getTimeWindow()).thenReturn(Optional.of(ImmutableTimeWindow.builder()
            .startTime(startTime)
            .endTime(startTime + 1_000)
            .build()));

        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        when(queryScope.getExpandedOids()).thenReturn(Collections.singleton(1L));
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(context).newPeriodInputDto(REQ_STATS);
        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(backend).getAveragedEntityStats(reqCaptor.capture());
        final GetAveragedEntityStatsRequest req = reqCaptor.getValue();
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getEntitiesList(), containsInAnyOrder(1L));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        // The same stat snapshot will be included in the results twice--once at the requested time,
        // and once at the "current" time.
        assertEquals(2, results.size());
        final List<StatSnapshotApiDTO> resultsAtStartTime = results.stream()
            .filter(statSnapshotApiDTO -> startTime == DateTimeUtil.parseTime(statSnapshotApiDTO.getDate()))
            .collect(Collectors.toList());
        assertEquals(1, resultsAtStartTime.size());
        final StatSnapshotApiDTO startTimeSnapshot = resultsAtStartTime.get(0);
        assertEquals(MAPPED_STAT_SNAPSHOT.getStatistics(), startTimeSnapshot.getStatistics());
        assertEquals(Epoch.HISTORICAL, startTimeSnapshot.getEpoch());
        final List<StatSnapshotApiDTO> resultsAtCurrentTime = results.stream()
            .filter(statSnapshotApiDTO -> currentTime == DateTimeUtil.parseTime(statSnapshotApiDTO.getDate()))
            .collect(Collectors.toList());
        assertEquals(1, resultsAtCurrentTime.size());
        final StatSnapshotApiDTO currentTimeSnapshot = resultsAtCurrentTime.get(0);
        assertEquals(MAPPED_STAT_SNAPSHOT.getStatistics(), currentTimeSnapshot.getStatistics());
        assertEquals(Epoch.CURRENT, currentTimeSnapshot.getEpoch());
    }

    @Test
    public void testlGroupStatsRequestWithNoOid() throws OperationFailedException {
        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getGlobalScope()).thenReturn(Optional.empty());
        //setting expandedOids to empty.
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(backend, never()).getAveragedEntityStats(any());
        assertThat(results.size(), is(0));
    }

    @Test
    public void testlGroupStatsRequestWithNoOidGlobal() throws OperationFailedException {
        // These entities in the scope.
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        //Setting to global scope.
        GlobalScope globalScope =  ImmutableGlobalScope.builder().build();
        when(queryScope.getGlobalScope()).thenReturn(Optional.of(globalScope));
        //setting expandedOids to empty.
        when(queryScope.getExpandedOids()).thenReturn(Collections.emptySet());
        when(context.getQueryScope()).thenReturn(queryScope);

        // ACT
        final List<StatSnapshotApiDTO> results = query.getAggregateStats(REQ_STATS, context);

        verify(backend, atLeastOnce()).getAveragedEntityStats(any());
        assertThat(results.size(), greaterThan(0));
    }
}
