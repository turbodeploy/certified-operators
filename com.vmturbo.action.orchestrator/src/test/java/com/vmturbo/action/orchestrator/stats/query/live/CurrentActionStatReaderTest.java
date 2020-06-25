package com.vmturbo.action.orchestrator.stats.query.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status.Code;

import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.query.live.CombinedStatsBuckets.CombinedStatsBucketsFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CurrentActionStatReaderTest {
    private QueryInfoFactory queryInfoFactory = mock(QueryInfoFactory.class);

    private CombinedStatsBucketsFactory statsBucketsFactory = mock(CombinedStatsBucketsFactory.class);

    private ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);

    private CurrentActionStatReader statReader;

    private static final Action ACTION = Action.newBuilder()
        .setId(1)
        .setInfo(ActionInfo.newBuilder()
            .setActivate(Activate.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(232)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
        .setExplanation(Explanation.getDefaultInstance())
        .setDeprecatedImportance(1)
        .build();

    /**
     * A move action which move vm from one host to another.
     */
    private static final Action MOVE_ACTION = Action.newBuilder()
        .setId(1)
        .setInfo(ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                    .setId(112)
                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionEntity.newBuilder()
                        .setId(221)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE))
                    .setDestination(ActionEntity.newBuilder()
                        .setId(222)
                        .setType(EntityType.PHYSICAL_MACHINE_VALUE)))))
        .setExplanation(Explanation.getDefaultInstance())
        .setDeprecatedImportance(1)
        .build();

    @Before
    public void setup() {
        statReader = new CurrentActionStatReader(queryInfoFactory, statsBucketsFactory,
            actionStorehouse);
    }

    @Test
    public void testReadActionStatsNoStoreForContext() {
        final long context1 = 182;
        final SingleQuery query1 = SingleQuery.newBuilder()
            .setQueryId(1)
            .setQuery(CurrentActionStatsQuery.getDefaultInstance())
            .build();
        final QueryInfo queryInfo1 = mock(QueryInfo.class);
        when(queryInfo1.queryId()).thenReturn(query1.getQueryId());
        when(queryInfo1.topologyContextId()).thenReturn(context1);
        when(queryInfoFactory.extractQueryInfo(query1)).thenReturn(queryInfo1);

        when(actionStorehouse.getStore(context1)).thenReturn(Optional.empty());

        try {
            statReader.readActionStats(
                GetCurrentActionStatsRequest.newBuilder()
                    .addQueries(query1)
                    .build());
            Assert.fail("Expected exception!");
        } catch (FailedActionQueryException e) {
            assertThat(e.asGrpcException(),
                GrpcExceptionMatcher.hasCode(Code.NOT_FOUND).descriptionContains(Long.toString(context1)));
        }
    }

    @Test
    public void testReadActionStatsStoreDataAccessException() {
        final long context1 = 182;
        final SingleQuery query1 = SingleQuery.newBuilder()
            .setQueryId(1)
            .setQuery(CurrentActionStatsQuery.getDefaultInstance())
            .build();
        final QueryInfo queryInfo1 = mock(QueryInfo.class);
        when(queryInfo1.queryId()).thenReturn(query1.getQueryId());
        when(queryInfo1.topologyContextId()).thenReturn(context1);
        when(queryInfoFactory.extractQueryInfo(query1)).thenReturn(queryInfo1);

        final CombinedStatsBuckets bucket1 = mock(CombinedStatsBuckets.class);
        when(statsBucketsFactory.bucketsForQuery(queryInfo1)).thenReturn(bucket1);

        final ActionStore exceptionalActionStore = mock(ActionStore.class);
        when(actionStorehouse.getStore(context1)).thenReturn(Optional.of(exceptionalActionStore));

        when(exceptionalActionStore.getActionViews()).thenThrow(new DataAccessException("BOO!"));

        try {
            statReader.readActionStats(GetCurrentActionStatsRequest.newBuilder()
                .addQueries(query1)
                .build());
            Assert.fail("Expected failed query exception!");
        } catch (FailedActionQueryException e) {
            assertThat(e.asGrpcException(),
                GrpcExceptionMatcher.hasCode(Code.INTERNAL).descriptionContains("BOO!"));
        }
    }

    /**
     * Tests the basic function of readActionStats.
     *
     * @throws FailedActionQueryException should not be thrown.
     */
    @Test
    public void testReadActionStats() throws FailedActionQueryException {
        // Create two queries, with a different group by to distinguish them.
        final SingleQuery query1 = SingleQuery.newBuilder()
            .setQueryId(1)
            .setQuery(CurrentActionStatsQuery.getDefaultInstance())
            .build();
        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
            .setInvestments(1.0)
            .build();
        final CurrentActionStat stat2 = CurrentActionStat.newBuilder()
            .setInvestments(2.0)
            .build();
        final SingleQuery query2 = SingleQuery.newBuilder()
            .setQueryId(2)
            .setQuery(CurrentActionStatsQuery.getDefaultInstance())
            .build();

        final long context1 = 1;
        final long context2 = 2;
        final QueryInfo queryInfo1 = mock(QueryInfo.class);
        when(queryInfo1.queryId()).thenReturn(query1.getQueryId());
        when(queryInfo1.topologyContextId()).thenReturn(context1);
        when(queryInfo1.query()).thenReturn(query1.getQuery());
        when(queryInfo1.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES);
        when(queryInfoFactory.extractQueryInfo(query1)).thenReturn(queryInfo1);

        final QueryInfo queryInfo2 = mock(QueryInfo.class);
        when(queryInfo2.queryId()).thenReturn(query2.getQueryId());
        when(queryInfo2.topologyContextId()).thenReturn(context2);
        when(queryInfo2.query()).thenReturn(query2.getQuery());
        when(queryInfo2.involvedEntityCalculation()).thenReturn(InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES);
        when(queryInfoFactory.extractQueryInfo(query2)).thenReturn(queryInfo2);

        final CombinedStatsBuckets bucket1 = mock(CombinedStatsBuckets.class);
        final CombinedStatsBuckets bucket2 = mock(CombinedStatsBuckets.class);
        when(bucket1.toActionStats()).thenReturn(Stream.of(stat1));
        when(bucket2.toActionStats()).thenReturn(Stream.of(stat2));
        when(statsBucketsFactory.bucketsForQuery(queryInfo1)).thenReturn(bucket1);
        when(statsBucketsFactory.bucketsForQuery(queryInfo2)).thenReturn(bucket2);

        final ActionStore actionStore1 = mock(ActionStore.class);
        when(actionStore1.getStoreTypeName()).thenReturn("store1");
        final ActionStore actionStore2 = mock(ActionStore.class);
        when(actionStore2.getStoreTypeName()).thenReturn("store2");
        final Predicate<ActionView> visibilityPredicate1 = mock(Predicate.class);
        when(visibilityPredicate1.test(any())).thenReturn(true);
        Predicate<ActionView> visibilityPredicate2 = mock(Predicate.class);
        when(visibilityPredicate2.test(any())).thenReturn(true);
        when(actionStore1.getVisibilityPredicate()).thenReturn(visibilityPredicate1);
        when(actionStore2.getVisibilityPredicate()).thenReturn(visibilityPredicate2);
        when(actionStorehouse.getStore(context1)).thenReturn(Optional.of(actionStore1));
        when(actionStorehouse.getStore(context2)).thenReturn(Optional.of(actionStore2));

        final ActionView actionView1 = ActionOrchestratorTestUtils.mockActionView(ACTION);
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(ACTION);
        when(queryInfo1.viewPredicate()).thenReturn(actionInfo -> actionInfo.action() == actionView1);
        when(queryInfo2.viewPredicate()).thenReturn(actionInfo -> actionInfo.action() == actionView2);
        when(actionStore1.getActionViews()).thenReturn(new MapBackedActionViews(ImmutableMap.of(1L, actionView1)));
        when(actionStore2.getActionViews()).thenReturn(new MapBackedActionViews(ImmutableMap.of(2L, actionView2)));

        final GetCurrentActionStatsRequest req = GetCurrentActionStatsRequest.newBuilder()
            .addQueries(query1)
            .addQueries(query2)
            .build();

        final Map<Long, List<CurrentActionStat>> statsByQueryId = statReader.readActionStats(req);

        verify(queryInfoFactory).extractQueryInfo(query1);
        verify(queryInfoFactory).extractQueryInfo(query2);
        verify(statsBucketsFactory).bucketsForQuery(queryInfo1);
        verify(statsBucketsFactory).bucketsForQuery(queryInfo2);
        verify(actionStorehouse).getStore(context1);
        verify(actionStorehouse).getStore(context2);
        verify(bucket1).addActionInfo(any());
        verify(bucket2).addActionInfo(any());

        assertThat(statsByQueryId.get(query1.getQueryId()), contains(stat1));
        assertThat(statsByQueryId.get(query2.getQueryId()), contains(stat2));
    }

    /**
     * Test the case that action count is correct if the passed queries contain entities which
     * are involved in same action. For example: there is an action
     * "move vm 112 from host 221 to host 222", it passed two queries, one for host221, the other
     * one for host222, it should return action count 1 for both, not 2.
     *
     * @throws FailedActionQueryException any error happens
     */
    @Test
    public void testReadActionStatsForEntitiesWhichAreRelatedToSameAction()
            throws FailedActionQueryException {
        statReader = new CurrentActionStatReader(queryInfoFactory,
            new CombinedStatsBucketsFactory(), actionStorehouse);
        // Create two queries, for different entities scopes
        final SingleQuery query1 = SingleQuery.newBuilder()
            .setQueryId(221)
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setEntityList(EntityScope.newBuilder().addOids(221))))
            .build();
        final SingleQuery query2 = SingleQuery.newBuilder()
            .setQueryId(222)
            .setQuery(CurrentActionStatsQuery.newBuilder()
                .setScopeFilter(ScopeFilter.newBuilder()
                    .setEntityList(EntityScope.newBuilder().addOids(222))))
            .build();

        final long contextId = 1;
        final QueryInfo queryInfo1 = ImmutableQueryInfo.builder()
            .queryId(query1.getQueryId())
            .topologyContextId(contextId)
            .entityPredicate(entity -> entity.getId() == 221L)
            .query(query1.getQuery())
            .actionGroupPredicate(action -> true)
            .desiredEntities(ImmutableSet.of(221L))
            .involvedEntityCalculation(InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES)
            .build();
        when(queryInfoFactory.extractQueryInfo(query1)).thenReturn(queryInfo1);

        final QueryInfo queryInfo2 = ImmutableQueryInfo.builder()
            .queryId(query2.getQueryId())
            .topologyContextId(contextId)
            .entityPredicate(entity -> entity.getId() == 222L)
            .query(query2.getQuery())
            .actionGroupPredicate(action -> true)
            .desiredEntities(ImmutableSet.of(222L))
            .involvedEntityCalculation(InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES)
            .build();
        when(queryInfoFactory.extractQueryInfo(query2)).thenReturn(queryInfo2);

        final ActionStore actionStore = mock(ActionStore.class);
        when(actionStore.getStoreTypeName()).thenReturn("store1");
        final Predicate<ActionView> visibilityPredicate = mock(Predicate.class);
        when(visibilityPredicate.test(any())).thenReturn(true);
        when(actionStore.getVisibilityPredicate()).thenReturn(visibilityPredicate);
        when(actionStorehouse.getStore(contextId)).thenReturn(Optional.of(actionStore));

        // mock move action: move vm 112 from host 221 to host 222
        ActionView actionView = ActionOrchestratorTestUtils.mockActionView(MOVE_ACTION);
        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(
            ImmutableMap.of(1111L, actionView)));

        final GetCurrentActionStatsRequest req = GetCurrentActionStatsRequest.newBuilder()
                .addQueries(query1)
                .addQueries(query2)
                .build();
        final Map<Long, List<CurrentActionStat>> statsByQueryId = statReader.readActionStats(req);

        // verify that action count for host 221 and host 222 are both 1, not 2
        assertThat(statsByQueryId.get(query1.getQueryId()).get(0).getActionCount(), is(1));
        assertThat(statsByQueryId.get(query2.getQueryId()).get(0).getActionCount(), is(1));
    }
}
