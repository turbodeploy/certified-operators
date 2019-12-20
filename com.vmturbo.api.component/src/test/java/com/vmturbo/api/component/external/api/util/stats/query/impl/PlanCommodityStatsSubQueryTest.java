package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsTestUtil;
import com.vmturbo.api.component.external.api.util.stats.query.impl.PlanCommodityStatsSubQuery.RequestMapper;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

public class PlanCommodityStatsSubQueryTest {
    private static final long MILLIS = 1_000_000;
    private static final long SCOPE_ID = 777;

    private static final GetAveragedEntityStatsRequest MAPPED_REQUEST =
        GetAveragedEntityStatsRequest.newBuilder()
            // Some value for uniqueness.
            .addEntities(1231)
            .build();

    private static final StatsFilter FILTER = StatsFilter.newBuilder()
        // For uniqueness/equality comparison
        .setStartDate(1L)
        .build();

    private static final StatSnapshot HISTORY_STAT_SNAPSHOT = StatSnapshot.newBuilder()
        // For uniqueness/equality comparison.
        .setSnapshotDate(MILLIS)
        .build();

    private static final Set<StatApiInputDTO> REQ_STATS =
        Collections.singleton(StatsTestUtil.statInput("foo"));

    private final RequestMapper mockRequestMapper = mock(RequestMapper.class);

    private final StatsMapper statsMapper = mock(StatsMapper.class);

    private final StatsHistoryServiceMole backend = spy(StatsHistoryServiceMole.class);

    @Captor
    public ArgumentCaptor<Set<StatApiInputDTO>> finalApiReqCaptor;

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PlanCommodityStatsSubQuery query;

    private final ApiId scope = mock(ApiId.class);

    private final StatsQueryContext context = mock(StatsQueryContext.class);

    private static final StatPeriodApiInputDTO NEW_PERIOD_INPUT_DTO = new StatPeriodApiInputDTO();

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        query = new PlanCommodityStatsSubQuery(statsMapper,
            StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            mockRequestMapper);

        when(scope.oid()).thenReturn(SCOPE_ID);

        when(context.getInputScope()).thenReturn(scope);

        when(statsMapper.newPeriodStatsFilter(any())).thenReturn(FILTER);

        final StatSnapshotApiDTO mappedStatSnapshot;
        mappedStatSnapshot = new StatSnapshotApiDTO();
        mappedStatSnapshot.setDate(DateTimeUtil.toString(MILLIS));
        mappedStatSnapshot.setEpoch(Epoch.PLAN_PROJECTED);
        mappedStatSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));
        when(statsMapper.toStatSnapshotApiDTO(HISTORY_STAT_SNAPSHOT)).thenReturn(mappedStatSnapshot);

        doReturn(Collections.singletonList(HISTORY_STAT_SNAPSHOT))
            .when(backend).getAveragedEntityStats(any());

        when(context.newPeriodInputDto(any())).thenReturn(NEW_PERIOD_INPUT_DTO);
    }

    @Test
    public void testApplicableWhenScopePlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(true));
    }

    @Test
    public void testNotApplicableWhenScopeNotPlan() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertThat(query.applicableInContext(context), is(false));
    }

    @Test
    public void testMapIncludeSource() {
        when(context.includeCurrent()).thenReturn(true);
        when(context.requestProjected()).thenReturn(false);

        RequestMapper requestMapper = new RequestMapper(statsMapper);
        final GetAveragedEntityStatsRequest req = requestMapper.toAveragedEntityStatsRequest(REQ_STATS, context);

        assertThat(req.getEntitiesList(), containsInAnyOrder(SCOPE_ID));
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(context).newPeriodInputDto(finalApiReqCaptor.capture());
        final Set<StatApiInputDTO> finalApiReq = finalApiReqCaptor.getValue();
        assertThat(finalApiReq.size(), is(1));
        assertThat(finalApiReq.iterator().next().getName(), is("currentFoo"));
    }

    @Test
    public void testMapIncludeProjected() {
        when(context.includeCurrent()).thenReturn(false);
        when(context.requestProjected()).thenReturn(true);

        // ACT
        final GetAveragedEntityStatsRequest req = new RequestMapper(statsMapper)
            .toAveragedEntityStatsRequest(REQ_STATS, context);

        assertThat(req.getEntitiesList(), containsInAnyOrder(SCOPE_ID));
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(context).newPeriodInputDto(finalApiReqCaptor.capture());
        final Set<StatApiInputDTO> finalApiReq = finalApiReqCaptor.getValue();

        // No changes - just "foo".
        assertThat(finalApiReq, is(REQ_STATS));
    }

    @Test
    public void testMapIncludeCurrentAndProjected() {
        when(context.includeCurrent()).thenReturn(true);
        when(context.requestProjected()).thenReturn(true);

        // ACT
        final GetAveragedEntityStatsRequest req = new RequestMapper(statsMapper)
            .toAveragedEntityStatsRequest(REQ_STATS, context);

        assertThat(req.getEntitiesList(), containsInAnyOrder(SCOPE_ID));
        assertThat(req.getFilter(), is(FILTER));
        assertThat(req.getGlobalFilter(), is(GlobalFilter.getDefaultInstance()));

        verify(statsMapper).newPeriodStatsFilter(NEW_PERIOD_INPUT_DTO);

        verify(context).newPeriodInputDto(finalApiReqCaptor.capture());
        final Set<StatApiInputDTO> finalApiReq = finalApiReqCaptor.getValue();
        assertThat(finalApiReq.size(), is(2));
        assertThat(finalApiReq.stream()
            .map(StatApiInputDTO::getName)
            .collect(Collectors.toSet()), containsInAnyOrder("foo", "currentFoo"));
    }

    @Test
    public void testStatsRequestSeparateCurrentAndProjected() throws OperationFailedException {
        when(mockRequestMapper.toAveragedEntityStatsRequest(REQ_STATS, context))
            .thenReturn(MAPPED_REQUEST);

        final long planStartTime = MILLIS;
        final long planEndTime = MILLIS + 1000;
        final StatSnapshot curSnapshot = StatSnapshot.newBuilder()
            .setSnapshotDate(planStartTime)
            .setStatEpoch(StatEpoch.PLAN_SOURCE)
            .addStatRecords(StatRecord.newBuilder()
                .setName("currentFoo"))
            .build();
        final StatSnapshot projSnapshot = StatSnapshot.newBuilder()
            .setSnapshotDate(planEndTime)
            .setStatEpoch(StatEpoch.PLAN_PROJECTED)
            .addStatRecords(StatRecord.newBuilder()
                .setName("foo"))
            .build();

        doReturn(Lists.newArrayList(curSnapshot, projSnapshot))
            .when(backend).getAveragedEntityStats(any());

        final StatSnapshotApiDTO mappedSourceSnapshot;
        mappedSourceSnapshot = new StatSnapshotApiDTO();
        mappedSourceSnapshot.setDate(DateTimeUtil.toString(planStartTime));
        mappedSourceSnapshot.setEpoch(Epoch.PLAN_SOURCE);
        mappedSourceSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));
        final StatSnapshotApiDTO mappedProjectedSnapshot;
        mappedProjectedSnapshot = new StatSnapshotApiDTO();
        mappedProjectedSnapshot.setDate(DateTimeUtil.toString(planEndTime));
        mappedProjectedSnapshot.setEpoch(Epoch.PLAN_PROJECTED);
        mappedProjectedSnapshot.setStatistics(Collections.singletonList(StatsTestUtil.stat("foo")));
        when(statsMapper.toStatSnapshotApiDTO(curSnapshot)).thenReturn(mappedSourceSnapshot);
        when(statsMapper.toStatSnapshotApiDTO(projSnapshot)).thenReturn(mappedProjectedSnapshot);

        // ACT
        final List<StatSnapshotApiDTO> response = query.getAggregateStats(REQ_STATS, context);

        assertThat(response.stream()
                .map(StatSnapshotApiDTO::getDate)
                .map(DateTimeUtil::parseTime)
                .collect(Collectors.toList()),
            containsInAnyOrder(planStartTime, planEndTime));
        assertThat(response.stream()
                .map(StatSnapshotApiDTO::getEpoch)
                .collect(Collectors.toList()),
            containsInAnyOrder(Epoch.PLAN_SOURCE, Epoch.PLAN_PROJECTED));
        assertThat(response.stream()
                .filter(statSnapshotApiDTO -> Epoch.PLAN_SOURCE == statSnapshotApiDTO.getEpoch())
                .map(StatSnapshotApiDTO::getStatistics)
                .findFirst()
                .get(),
            is(mappedSourceSnapshot.getStatistics()));
        assertThat(response.stream()
                .filter(statSnapshotApiDTO -> Epoch.PLAN_PROJECTED == statSnapshotApiDTO.getEpoch())
                .map(StatSnapshotApiDTO::getStatistics)
                .findFirst()
                .get(),
            is(mappedProjectedSnapshot.getStatistics()));
    }

}