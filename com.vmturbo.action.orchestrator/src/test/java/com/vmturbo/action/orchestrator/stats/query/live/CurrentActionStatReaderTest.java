package com.vmturbo.action.orchestrator.stats.query.live;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.jooq.exception.DataAccessException;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.Status.Code;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.query.live.CombinedStatsBuckets.CombinedStatsBucketsFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CurrentActionStatReaderTest {
    private QueryInfoFactory queryInfoFactory = mock(QueryInfoFactory.class);

    private CombinedStatsBucketsFactory statsBucketsFactory = mock(CombinedStatsBucketsFactory.class);

    private ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);

    private ActionTranslator actionTranslator = mock(ActionTranslator.class);

    private CurrentActionStatReader statReader = new CurrentActionStatReader(queryInfoFactory,
        statsBucketsFactory,
        actionStorehouse,
        actionTranslator);

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

    @Test
    public void testReadActionStatsNoStoreForContext() throws FailedActionQueryException {
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
    public void testReadActionStatsStoreDataAccessException() throws FailedActionQueryException {
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
        when(queryInfoFactory.extractQueryInfo(query1)).thenReturn(queryInfo1);

        final QueryInfo queryInfo2 = mock(QueryInfo.class);
        when(queryInfo2.queryId()).thenReturn(query2.getQueryId());
        when(queryInfo2.topologyContextId()).thenReturn(context2);
        when(queryInfo2.query()).thenReturn(query2.getQuery());
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

        doAnswer(invocation -> invocation.getArgumentAt(0, Stream.class))
            .when(actionTranslator).translate(isA(Stream.class));

        final ActionView actionView1 = mock(ActionView.class);
        final ActionView actionView2 = mock(ActionView.class);
        when(queryInfo1.viewPredicate()).thenReturn(actionInfo -> actionInfo.action() == actionView1);
        when(queryInfo2.viewPredicate()).thenReturn(actionInfo -> actionInfo.action() == actionView2);
        when(actionView1.getRecommendation()).thenReturn(ACTION);
        when(actionView2.getRecommendation()).thenReturn(ACTION);
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

        assertThat(statsByQueryId.get(query1.getQueryId()), contains(stat1));
        assertThat(statsByQueryId.get(query2.getQueryId()), contains(stat2));
    }

}