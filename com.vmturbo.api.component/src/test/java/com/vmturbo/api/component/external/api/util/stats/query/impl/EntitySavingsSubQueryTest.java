package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.Cost.GetEntitySavingsStatsRequest;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostREST;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit tests for EntitySavingsSubQuery.
 */
public class EntitySavingsSubQueryTest {
    private CostServiceMole costServiceMole = spy(new CostServiceMole());

    /**
     * Test server.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costServiceMole);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    private EntitySavingsSubQuery query;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        query = spy(new EntitySavingsSubQuery(costRpc, groupExpander));
    }

    /**
     * Test the applicableInPlan method.
     * EntitySavingsSubQuery is applicable only if scope is not a plan and is a cloud scope.
     */
    @Test
    public void testApplicableInContext() {
        final ApiId scope = mock(ApiId.class);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isCloud()).thenReturn(true);

        final StatsQueryContext context = mock(StatsQueryContext.class);
        when(context.getInputScope()).thenReturn(scope);

        assertEquals(true, query.applicableInContext(context));

        when(scope.isPlan()).thenReturn(true);
        when(scope.isCloud()).thenReturn(true);
        assertEquals(false, query.applicableInContext(context));

        when(scope.isPlan()).thenReturn(true);
        when(scope.isCloud()).thenReturn(false);
        assertEquals(false, query.applicableInContext(context));

        when(scope.isPlan()).thenReturn(false);
        when(scope.isCloud()).thenReturn(false);
        when(scope.isHybridGroup()).thenReturn(false);
        assertEquals(false, query.applicableInContext(context));

        when(scope.isPlan()).thenReturn(false);
        when(scope.isCloud()).thenReturn(false);
        when(scope.isHybridGroup()).thenReturn(true);
        assertEquals(true, query.applicableInContext(context));
    }

    /**
     * Test for getAggregateSats method.
     * Verify the input to the cost component getEntitySavingsStats protobuf API.
     * Verify the conversion of the protobuf response from cost component to StatSnapshotApiDTO.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGetAggregateStats() throws Exception {
        long vm1Id = 123456L;
        long vm2Id = 234543L;
        long startTime = 1609527257000L;
        long endTime = 1610996057000L;

        TimeWindow timeWindow = ImmutableTimeWindow.builder().startTime(startTime).endTime(endTime)
                .includeHistorical(true).includeCurrent(false).includeProjected(false).build();

        Set<Long> scopeIds = new HashSet<>();
        scopeIds.add(vm1Id);
        scopeIds.add(vm2Id);
        Set<ApiEntityType> entityTypes = ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE);
        StatsQueryContext context = createStatsQueryContextMock(scopeIds, timeWindow, entityTypes);
        List<EntityStatsResponseValues> responseValues = Arrays.asList(
            new EntityStatsResponseValues(1610960400000L, 10.0f, 1.0f),
            new EntityStatsResponseValues(1610964000000L, 20.0f, 2.0f),
            new EntityStatsResponseValues(1610967600000L, 30.0f, 3.0f));

        when(costServiceMole.getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class)))
                .thenReturn(createEntityStatsResponse(responseValues));

        List<StatSnapshotApiDTO> response = query.getAggregateStats(createStatApiInputDTOs(), context);

        // Verify the input for the cost getEntitySavingsStats API.
        verifyInputToProtobuf(startTime, endTime, scopeIds, entityTypes);

        // Verify the conversion of the protobuf response to API DTO.
        assertEquals(3, response.size());
        for (int i = 0; i < 3; i++) {
            StatSnapshotApiDTO statSnapshot = response.get(i);
            EntityStatsResponseValues expectedValue = responseValues.get(i);
            assertEquals(DateTimeUtil.toString(expectedValue.snapshotTime), statSnapshot.getDate());
            for (StatApiDTO statApiDTO : statSnapshot.getStatistics()) {
                if (statApiDTO.getName().equals(EntitySavingsStatsType.REALIZED_SAVINGS.name())) {
                    assertEquals(expectedValue.savingValue, statApiDTO.getValue(), 0);
                } else if (statApiDTO.getName().equals(EntitySavingsStatsType.REALIZED_INVESTMENTS.name())) {
                    assertEquals(expectedValue.investmentValue, statApiDTO.getValue(), 0);
                }
            }
        }
    }

    private void verifyInputToProtobuf(long startTime, long endTime, Set<Long> scopeIds, Set<ApiEntityType> entityTypes) {
        ArgumentCaptor<GetEntitySavingsStatsRequest> requestArgumentCaptor
                = ArgumentCaptor.forClass(GetEntitySavingsStatsRequest.class);
        verify(costServiceMole).getEntitySavingsStats(requestArgumentCaptor.capture());

        // Verify the request for the cost protobuf api for getting entity savings stats.
        assertEquals(startTime, requestArgumentCaptor.getValue().getStartDate());
        assertEquals(endTime, requestArgumentCaptor.getValue().getEndDate());

        assertEquals(2, requestArgumentCaptor.getValue().getStatsTypesCount());
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_SAVINGS));
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_INVESTMENTS));

        assertEquals(2, requestArgumentCaptor.getValue().getEntityFilter().getEntityIdCount());
        assertTrue(requestArgumentCaptor.getValue().getEntityFilter().getEntityIdList().containsAll(scopeIds));
        Set<Integer> entityTypeNumbers = entityTypes.stream().map(ApiEntityType::typeNumber).collect(Collectors.toSet());
        assertTrue(requestArgumentCaptor.getValue().getEntityTypeFilter().getEntityTypeIdList().containsAll(entityTypeNumbers));
        assertTrue(requestArgumentCaptor.getValue().getResourceGroupFilter().getResourceGroupOidList().isEmpty());
    }

    /**
     * Test calling aggregateState for resource groups.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGetAggregateStatsResourceGroups() throws Exception {
        long rgId = 10000L;
        long startTime = 1609527257000L;
        long endTime = 1610996057000L;

        TimeWindow timeWindow = ImmutableTimeWindow.builder().startTime(startTime).endTime(endTime)
                .includeHistorical(true).includeCurrent(false).includeProjected(false).build();

        StatsQueryContext context = createResourceGroupStatsQueryContextMock(rgId, GroupType.RESOURCE, timeWindow);

        List<EntityStatsResponseValues> responseValues = Arrays.asList(
                new EntityStatsResponseValues(1610960400000L, 10.0f, 1.0f),
                new EntityStatsResponseValues(1610964000000L, 20.0f, 2.0f),
                new EntityStatsResponseValues(1610967600000L, 30.0f, 3.0f));

        when(costServiceMole.getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class)))
                .thenReturn(createEntityStatsResponse(responseValues));

        List<StatSnapshotApiDTO> response = query.getAggregateStats(createStatApiInputDTOs(), context);

        ArgumentCaptor<GetEntitySavingsStatsRequest> requestArgumentCaptor
                = ArgumentCaptor.forClass(GetEntitySavingsStatsRequest.class);
        verify(costServiceMole).getEntitySavingsStats(requestArgumentCaptor.capture());

        // Verify the request for the cost protobuf api for getting entity savings stats.
        assertEquals(startTime, requestArgumentCaptor.getValue().getStartDate());
        assertEquals(endTime, requestArgumentCaptor.getValue().getEndDate());

        assertEquals(2, requestArgumentCaptor.getValue().getStatsTypesCount());
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_SAVINGS));
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_INVESTMENTS));

        assertTrue(requestArgumentCaptor.getValue().getEntityFilter().getEntityIdList().isEmpty());
        assertTrue(requestArgumentCaptor.getValue().getEntityFilter().getEntityIdList().isEmpty());
        assertTrue(requestArgumentCaptor.getValue().getEntityTypeFilter().getEntityTypeIdList().isEmpty());
        assertEquals(1, requestArgumentCaptor.getValue().getResourceGroupFilter().getResourceGroupOidList().size());
        assertTrue(requestArgumentCaptor.getValue().getResourceGroupFilter().getResourceGroupOidList().contains(rgId));
    }

    /**
     * Test calling aggregateState for a group of resource groups.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGetAggregateStatsGroupOfResourceGroups() throws Exception {
        long groupId = 10000L;
        long startTime = 1609527257000L;
        long endTime = 1610996057000L;

        TimeWindow timeWindow = ImmutableTimeWindow.builder().startTime(startTime).endTime(endTime)
                .includeHistorical(true).includeCurrent(false).includeProjected(false).build();

        StatsQueryContext context = createResourceGroupStatsQueryContextMock(groupId, GroupType.REGULAR, timeWindow);

        Set<Long> rgIds = ImmutableSet.of(100L, 200L, 300L);
        GroupAndMembers rgs = ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder().setId(groupId).build())
                .members(rgIds)
                .entities(rgIds)
                .build();
        when(groupExpander.getGroupWithImmediateMembersOnly(Long.toString(groupId))).thenReturn(Optional.of(rgs));

        List<EntityStatsResponseValues> responseValues = Arrays.asList(
                new EntityStatsResponseValues(1610960400000L, 10.0f, 1.0f),
                new EntityStatsResponseValues(1610964000000L, 20.0f, 2.0f),
                new EntityStatsResponseValues(1610967600000L, 30.0f, 3.0f));

        when(costServiceMole.getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class)))
                .thenReturn(createEntityStatsResponse(responseValues));

        List<StatSnapshotApiDTO> response = query.getAggregateStats(createStatApiInputDTOs(), context);

        ArgumentCaptor<GetEntitySavingsStatsRequest> requestArgumentCaptor
                = ArgumentCaptor.forClass(GetEntitySavingsStatsRequest.class);
        verify(costServiceMole).getEntitySavingsStats(requestArgumentCaptor.capture());

        // Verify the request for the cost protobuf api for getting entity savings stats.
        assertEquals(startTime, requestArgumentCaptor.getValue().getStartDate());
        assertEquals(endTime, requestArgumentCaptor.getValue().getEndDate());

        assertEquals(2, requestArgumentCaptor.getValue().getStatsTypesCount());
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_SAVINGS));
        assertTrue(requestArgumentCaptor.getValue().getStatsTypesList().contains(EntitySavingsStatsType.REALIZED_INVESTMENTS));

        assertTrue(requestArgumentCaptor.getValue().getEntityFilter().getEntityIdList().isEmpty());
        assertTrue(requestArgumentCaptor.getValue().getEntityFilter().getEntityIdList().isEmpty());
        assertTrue(requestArgumentCaptor.getValue().getEntityTypeFilter().getEntityTypeIdList().isEmpty());
        assertEquals(3, requestArgumentCaptor.getValue().getResourceGroupFilter().getResourceGroupOidList().size());
        assertTrue(requestArgumentCaptor.getValue().getResourceGroupFilter().getResourceGroupOidList().containsAll(rgIds));
    }

    private Set<StatApiInputDTO> createStatApiInputDTOs() {
        StatApiInputDTO realizedSavingsStat = new StatApiInputDTO();
        realizedSavingsStat.setName(EntitySavingsStatsType.REALIZED_SAVINGS.name());

        StatApiInputDTO realizedInvestmentStat = new StatApiInputDTO();
        realizedInvestmentStat.setName(EntitySavingsStatsType.REALIZED_INVESTMENTS.name());

        Set<StatApiInputDTO> stats = new HashSet<>();
        stats.add(realizedSavingsStat);
        stats.add(realizedInvestmentStat);

        return stats;
    }

    private StatsQueryContext createStatsQueryContextMock(Set<Long> entityOids, TimeWindow timeWindow,
                                                          Set<ApiEntityType> entityTypes) {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId scope = mock(ApiId.class);
        when(scope.getScopeOids()).thenReturn(entityOids);
        if (entityTypes != null) {
            when(scope.getScopeTypes()).thenReturn(Optional.of(entityTypes));
        } else {
            when(scope.getScopeTypes()).thenReturn(Optional.empty());
        }
        when(context.getInputScope()).thenReturn(scope);
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));
        when(context.getInputScope().isRealtimeMarket()).thenReturn(true);
        when(context.getInputScope().uuid()).thenReturn("1");
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(Sets.newHashSet(1L));
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());
        return context;
    }

    private StatsQueryContext createResourceGroupStatsQueryContextMock(Long rgId, GroupType groupType, TimeWindow timeWindow) {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId scope = mock(ApiId.class);
        when(scope.getGroupType()).thenReturn(Optional.of(groupType));
        when(scope.isResourceGroupOrGroupOfResourceGroups()).thenReturn(true);
        when(scope.oid()).thenReturn(rgId);
        when(scope.getScopeTypes()).thenReturn(Optional.empty());
        when(context.getInputScope()).thenReturn(scope);
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));
        when(context.getInputScope().isRealtimeMarket()).thenReturn(true);
        when(context.getInputScope().uuid()).thenReturn("1");
        final StatsQueryScope queryScope = mock(StatsQueryScope.class);
        when(queryScope.getScopeOids()).thenReturn(Sets.newHashSet(1L));
        when(context.getQueryScope()).thenReturn(queryScope);
        when(context.getPlanInstance()).thenReturn(Optional.empty());
        return context;
    }

    private List<EntitySavingsStatsRecord> createEntityStatsResponse(List<EntityStatsResponseValues> values) {
        List<EntitySavingsStatsRecord> response = new ArrayList<>();

        for (EntityStatsResponseValues value : values) {
            response.add(createEntitySavingsStatsRecord(value.snapshotTime, value.savingValue, value.investmentValue));
        }

        return response;
    }

    private EntitySavingsStatsRecord createEntitySavingsStatsRecord(long snapshotTime,
                                                                    float savingValue,
                                                                    float investmentValue) {
        EntitySavingsStatsRecord.Builder responseBuilder = EntitySavingsStatsRecord.newBuilder();
        responseBuilder.setSnapshotDate(snapshotTime);

        SavingsRecord savingsRecord = SavingsRecord.newBuilder()
                .setName(CostREST.EntitySavingsStatsType.REALIZED_SAVINGS.name())
                .setValue(savingValue)
                .build();

        SavingsRecord investmentsRecord = SavingsRecord.newBuilder()
                .setName(CostREST.EntitySavingsStatsType.REALIZED_INVESTMENTS.name())
                .setValue(investmentValue)
                .build();

        responseBuilder.addStatRecords(savingsRecord);
        responseBuilder.addStatRecords(investmentsRecord);
        return responseBuilder.build();
    }

    /**
     * Simple class to hold values for the entity stats values.
     */
    private class EntityStatsResponseValues {
        long snapshotTime;
        float savingValue;
        float investmentValue;

        EntityStatsResponseValues(long snapshotTime, float savingValue, float investmentValue) {
            this.snapshotTime = snapshotTime;
            this.savingValue = savingValue;
            this.investmentValue = investmentValue;
        }
    }

    /**
     * If the stats request does not include a savings stats name (either the list is empty or
     * contains stats unrelated to savings), simply return an empty result list without calling
     * the cost component gRPC API.
     *
     * @throws Exception exceptions
     */
    @Test
    public void testNoValidSavingsStats() throws Exception {
        long vm1Id = 123456L;
        long vm2Id = 234543L;
        long startTime = 1609527257000L;
        long endTime = 1610996057000L;

        TimeWindow timeWindow = ImmutableTimeWindow.builder().startTime(startTime).endTime(endTime)
                .includeHistorical(true).includeCurrent(false).includeProjected(false).build();

        // Stats request has NO stats names.
        Set<Long> scopeIds = new HashSet<>();
        scopeIds.add(vm1Id);
        scopeIds.add(vm2Id);
        Set<ApiEntityType> entityTypes = ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE);
        StatsQueryContext context = createStatsQueryContextMock(scopeIds, timeWindow, entityTypes);

        Set<StatApiInputDTO> stats = new HashSet<>();
        List<StatSnapshotApiDTO> response = query.getAggregateStats(stats, context);

        verify(costServiceMole, never()).getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class));
        assertTrue(response.isEmpty());

        // Stats request include a stats that has unrelated to savings.
        StatApiInputDTO unknownStat = new StatApiInputDTO();
        unknownStat.setName("ABC");
        stats = new HashSet<>();
        stats.add(unknownStat);
        response = query.getAggregateStats(stats, context);
        verify(costServiceMole, never()).getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class));
        assertTrue(response.isEmpty());
    }
}
