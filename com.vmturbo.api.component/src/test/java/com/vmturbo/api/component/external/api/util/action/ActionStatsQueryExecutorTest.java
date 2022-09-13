package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

public class ActionStatsQueryExecutorTest {

    private static final long CSP_OID = 123L;
    private static final long TARGET_OID = 111L;
    private static final long PROBE_OID = 222L;

    private ActionsServiceMole actionsServiceMole = spy(new ActionsServiceMole());

    private HistoricalQueryMapper historicalQueryMapper = mock(HistoricalQueryMapper.class);
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private UuidMapper uuidMapper = mock(UuidMapper.class);
    private CurrentQueryMapper currentQueryMapper = mock(CurrentQueryMapper.class);
    private ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);
    private RepositoryApi repositoryApi = mock(RepositoryApi.class);
    private ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);
    private MutableFixedClock clock = new MutableFixedClock(1_000_000);
    private final CloudTypeMapper cloudTypeMapper = new CloudTypeMapper();
    private ActionStatsMapper actionStatsMapper = spy(new ActionStatsMapper(clock, actionSpecMapper));
    private ActionStatsQueryExecutor executor;

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceMole);

    @Before
    public void setup() {
        executor = new ActionStatsQueryExecutor(clock, ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            userSessionContext, uuidMapper, historicalQueryMapper, currentQueryMapper, actionStatsMapper, repositoryApi,
            thinTargetCache, cloudTypeMapper);
    }

    @Test
    public void testExecuteHistoricalQuery() throws OperationFailedException {
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
            .addMgmtUnitIds(1L)
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
        verify(currentQueryMapper, never()).mapToCurrentQueries(any());
    }

    @Test
    public void testExecuteCurrentQueryOnly() throws OperationFailedException {
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
        doReturn(translatedLiveStat).when(actionStatsMapper).currentActionStatsToApiSnapshot(
            Collections.singletonList(currentActionStat), actionStatsQuery, Maps.newHashMap(), Maps.newHashMap());
        doReturn(GetCurrentActionStatsResponse.newBuilder()
            .addResponses(GetCurrentActionStatsResponse.SingleResponse.newBuilder()
                .setQueryId(scopeId.oid())
                .addActionStats(currentActionStat))
            .build()).when(actionsServiceMole).getCurrentActionStats(any());


        final Map<ApiId, List<StatSnapshotApiDTO>> snapshots =
            executor.retrieveActionStats(actionStatsQuery);

        assertThat(snapshots.keySet(), contains(scopeId));
        assertThat(snapshots.get(scopeId), contains(translatedLiveStat));
        verify(historicalQueryMapper, never()).mapToHistoricalQueries(any());
    }

    /**
     * Test that {@link ActionStatsQueryExecutor#retrieveActionStats(ActionStatsQuery)}
     * contains {@link StatSnapshotApiDTO} containing filters for GCP CSP, when the input query contains a group by
     * CSP clause.
     */
    @Test
    public void testRetrieveActionStatsGroupByCSP() throws OperationFailedException {
        // given
        final ActionStatsQuery actionStatsQuery = mockActionStatsQuery();
        mockThinTargetCache();
        mockRepositoryApi();
        final ApiId scopeId = ApiTestUtils.mockEntityId("1", uuidMapper);
        mockCurrentQueryMapper(actionStatsQuery, scopeId);
        mockActionService(scopeId);
        // when
        final Map<ApiId, List<StatSnapshotApiDTO>> snapshots = executor.retrieveActionStats(actionStatsQuery);
        // then
        verifyStatSnapshotContainsCSPFilter(snapshots.getOrDefault(scopeId, Collections.emptyList()));
    }

    private ActionStatsQuery mockActionStatsQuery() {
        final ActionStatsQuery actionStatsQuery = mock(ActionStatsQuery.class);
        when(actionStatsQuery.isHistorical(clock)).thenReturn(false);
        when(actionStatsQuery.currentTimeStamp()).thenReturn(Optional.of(Long.toString(System.currentTimeMillis())));
        when(actionStatsQuery.getCostType()).thenReturn(Optional.of(ActionCostType.SAVING));
        ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Collections.singletonList("CSP"));
        when(actionStatsQuery.actionInput()).thenReturn(inputDTO);
        return actionStatsQuery;
    }

    private void mockThinTargetCache() {
        when(thinTargetCache.getTargetInfo(TARGET_OID))
            .thenReturn(Optional.of(ImmutableThinTargetInfo.builder().probeInfo(ImmutableThinProbeInfo.builder()
                    .type(SDKProbeType.GCP_PROJECT.getProbeType()).oid(PROBE_OID)
                    .uiCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                    .category(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                    .build())
                .oid(TARGET_OID)
                .isHidden(false)
                .displayName("gcp-project")
                .build()));
    }

    private void mockRepositoryApi() {
        final RepositoryApi.MultiEntityRequest multiEntityRequest = mock(RepositoryApi.MultiEntityRequest.class);
        final TopologyDTO.PartialEntity.ApiPartialEntity apiPartialEntity = TopologyDTO.PartialEntity.ApiPartialEntity
            .newBuilder().setOid(CSP_OID)
            .setOrigin(TopologyDTO.TopologyEntityDTO.Origin.newBuilder()
                .setDiscoveryOrigin(TopologyDTO.TopologyEntityDTO.DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(TARGET_OID, TopologyDTO.PerTargetEntityInformation.newBuilder().build())
                    .build())
                .build())
            .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE).build();
        when(multiEntityRequest.getEntities()).thenReturn(Stream.of(apiPartialEntity));
        when(repositoryApi.entitiesRequest(anySetOf(Long.class))).thenReturn(multiEntityRequest);
    }

    private void mockCurrentQueryMapper(final ActionStatsQuery actionStatsQuery,
                                        final ApiId scopeId) throws OperationFailedException {
        final CurrentActionStatsQuery grpcQuery = CurrentActionStatsQuery.newBuilder()
            .setActionGroupFilter(ActionGroupFilter.newBuilder()
                .addActionMode(ActionMode.MANUAL))
            .addGroupBy(CurrentActionStatsQuery.GroupBy.CSP)
            .build();
        when(currentQueryMapper.mapToCurrentQueries(actionStatsQuery))
            .thenReturn(ImmutableMap.of(scopeId, grpcQuery));
    }

    private void mockActionService(final ApiId scopeId) {
        final CurrentActionStat currentActionStat = CurrentActionStat.newBuilder()
            .setStatGroup(CurrentActionStat.StatGroup.newBuilder()
                .setCsp(Long.toString(CSP_OID))
                .build())
            .setSavings(1)
            .build();
        doReturn(GetCurrentActionStatsResponse.newBuilder()
            .addResponses(GetCurrentActionStatsResponse.SingleResponse.newBuilder()
                .setQueryId(scopeId.oid())
                .addActionStats(currentActionStat))
            .build()).when(actionsServiceMole).getCurrentActionStats(any());
    }

    private void verifyStatSnapshotContainsCSPFilter(final List<StatSnapshotApiDTO> resultantStatSnapshotApiDTOs) {
        Assert.assertFalse(resultantStatSnapshotApiDTOs.isEmpty());
        final StatSnapshotApiDTO statSnapshotApiDTO = resultantStatSnapshotApiDTOs.iterator().next();
        Assert.assertFalse(statSnapshotApiDTO.getStatistics().isEmpty());
        final StatApiDTO statApiDTO = statSnapshotApiDTO.getStatistics().iterator().next();
        Assert.assertTrue(statApiDTO.getFilters().stream().anyMatch(filter -> filter.getType().equals("CSP")
            && filter.getValue().equals("GCP")));
    }
}
