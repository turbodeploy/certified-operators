package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;

public class ActionStatsQueryExecutorTest {

    private ActionsServiceMole actionsServiceMole = spy(new ActionsServiceMole());

    private HistoricalQueryMapper historicalQueryMapper = mock(HistoricalQueryMapper.class);
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private UuidMapper uuidMapper = mock(UuidMapper.class);
    private CurrentQueryMapper currentQueryMapper = mock(CurrentQueryMapper.class);
    private ActionStatsMapper actionStatsMapper = mock(ActionStatsMapper.class);
    private RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceMole);

    @Test
    public void testExecuteHistoricalQuery() throws OperationFailedException {
        final ActionStatsQueryExecutor executor = new ActionStatsQueryExecutor(clock,
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            userSessionContext,
            uuidMapper,
            historicalQueryMapper,
            currentQueryMapper,
            actionStatsMapper,
            repositoryApi);
        final ActionStatsQuery actionStatsQuery = mock(ActionStatsQuery.class);
        ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(new ArrayList<>());
        when(actionStatsQuery.actionInput()).thenReturn(inputDTO);
        when(actionStatsQuery.isHistorical(clock)).thenReturn(true);
        EntityAccessScope userScope = mock(EntityAccessScope.class);
        when(userScope.containsAll()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);
        final HistoricalActionStatsQuery grpcQuery = HistoricalActionStatsQuery.newBuilder()
            .setGroupBy(GroupBy.ACTION_CATEGORY)
            .build();
        final ApiId apiId = ApiTestUtils.mockGroupId("1", uuidMapper);
        final Map<ApiId, HistoricalActionStatsQuery> grpcQueries = ImmutableMap.of(apiId, grpcQuery);

        final ActionStats actionStats = ActionStats.newBuilder()
            .setMgmtUnitId(1)
            .build();

        when(historicalQueryMapper.mapToHistoricalQueries(actionStatsQuery))
            .thenReturn(grpcQueries);
        when(actionsServiceMole.getHistoricalActionStats(any()))
            .thenReturn(GetHistoricalActionStatsResponse.newBuilder()
                .addResponses(GetHistoricalActionStatsResponse.SingleResponse.newBuilder()
                    .setQueryId(1)
                    .setActionStats(actionStats))
                .build());

        final StatSnapshotApiDTO mappedSnapshot = new StatSnapshotApiDTO();
        mappedSnapshot.setDate("foo");
        final List<StatSnapshotApiDTO> expectedRetStats =
            Collections.singletonList(mappedSnapshot);
        when(actionStatsMapper.historicalActionStatsToApiSnapshots(actionStats, actionStatsQuery))
            .thenReturn(expectedRetStats);

        final Map<ApiId, List<StatSnapshotApiDTO>> retStats = executor.retrieveActionStats(actionStatsQuery);

        assertThat(retStats.keySet(), contains(apiId));
        assertThat(retStats.get(apiId), is(expectedRetStats));
    }

    @Test
    public void testExecuteCurrentQueryOnly() throws OperationFailedException {
        final ActionStatsQueryExecutor executor = new ActionStatsQueryExecutor(clock,
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            userSessionContext,
            uuidMapper,
            historicalQueryMapper,
            currentQueryMapper,
            actionStatsMapper,
            repositoryApi);
        final ActionStatsQuery actionStatsQuery = mock(ActionStatsQuery.class);
        when(actionStatsQuery.isHistorical(clock)).thenReturn(false);
        ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(new ArrayList<>());
        when(actionStatsQuery.actionInput()).thenReturn(inputDTO);

        CurrentActionStatsQuery grpcQuery = CurrentActionStatsQuery.newBuilder()
            .setActionGroupFilter(ActionGroupFilter.newBuilder()
                .addActionMode(ActionMode.MANUAL))
            .build();
        final ApiId scopeId = ApiTestUtils.mockEntityId("1", uuidMapper);
        when(currentQueryMapper.mapToCurrentQueries(actionStatsQuery))
            .thenReturn(ImmutableMap.of(scopeId, grpcQuery));

        final CurrentActionStat currentActionStat = CurrentActionStat.newBuilder()
            .setInvestments(1)
            .build();

        final StatSnapshotApiDTO translatedLiveStat = new StatSnapshotApiDTO();
        when(actionStatsMapper.currentActionStatsToApiSnapshot(
                Collections.singletonList(currentActionStat), actionStatsQuery, Maps.newHashMap()))
            .thenReturn(translatedLiveStat);

        doReturn(GetCurrentActionStatsResponse.newBuilder()
            .addResponses(GetCurrentActionStatsResponse.SingleResponse.newBuilder()
                .setQueryId(scopeId.oid())
                .addActionStats(currentActionStat))
            .build()).when(actionsServiceMole).getCurrentActionStats(any());


        final Map<ApiId, List<StatSnapshotApiDTO>> snapshots =
            executor.retrieveActionStats(actionStatsQuery);

        assertThat(snapshots.keySet(), contains(scopeId));
        assertThat(snapshots.get(scopeId), contains(translatedLiveStat));
    }
}
