package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
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
import com.vmturbo.components.api.test.GrpcTestServer;

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

    private EntitySavingsSubQuery query;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        CostServiceBlockingStub costRpc = CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        query = spy(new EntitySavingsSubQuery(costRpc));
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
        StatsQueryContext context = createStatsQueryContextMock(scopeIds, timeWindow);
        List<EntityStatsResponseValues> responseValues = Arrays.asList(
            new EntityStatsResponseValues(1610960400000L, 10.0f, 1.0f),
            new EntityStatsResponseValues(1610964000000L, 20.0f, 2.0f),
            new EntityStatsResponseValues(1610967600000L, 30.0f, 3.0f));

        when(costServiceMole.getEntitySavingsStats(any(GetEntitySavingsStatsRequest.class)))
                .thenReturn(createEntityStatsResponse(responseValues));

        List<StatSnapshotApiDTO> response = query.getAggregateStats(createStatApiInputDTOs(), context);

        // Verify the input for the cost getEntitySavingsStats API.
        verifyInputToProtobuf(startTime, endTime, scopeIds);

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

    private void verifyInputToProtobuf(long startTime, long endTime, Set<Long> scopeIds) {
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

    private StatsQueryContext createStatsQueryContextMock(Set<Long> scopeOids, TimeWindow timeWindow) {
        final StatsQueryContext context = mock(StatsQueryContext.class);
        final ApiId scope = mock(ApiId.class);
        when(scope.getScopeOids()).thenReturn(scopeOids);
        when(context.getInputScope()).thenReturn(scope);
        when(context.getTimeWindow()).thenReturn(Optional.of(timeWindow));
        when(context.getInputScope().isRealtimeMarket()).thenReturn(false);
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
}
