package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesCache.Snapshot;
import com.vmturbo.action.orchestrator.store.LiveActions.QueryFilterFactory;
import com.vmturbo.action.orchestrator.store.query.QueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.components.api.test.MutableFixedClock;

public class LiveActionsTest {

    private ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    private EntitiesCache entitiesCache = mock(EntitiesCache.class);

    private Clock clock = new MutableFixedClock(1_000_000);

    private final QueryFilterFactory queryFilterFactory = mock(QueryFilterFactory.class);

    private LiveActions liveActions =
        new LiveActions(actionHistoryDao, entitiesCache, clock, queryFilterFactory);

    @Test
    public void testReplaceMarketActions() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        assertThat(liveActions.getAll().collect(Collectors.toList()),
            containsInAnyOrder(action1, action2));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.MARKET),
            containsInAnyOrder(action1, action2));
    }
    @Test
    public void testDoForEach() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        final List<Action> gotActions = new ArrayList<>();
        liveActions.doForEachMarketAction(gotActions::add);
        assertThat(gotActions, containsInAnyOrder(action1, action2));
    }

    @Test
    public void replaceRiActions() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);

        // Hack - for testing purposes we don't care if it's not actually an RI action.
        liveActions.replaceRiActions(Stream.of(action1, action2));
        assertThat(liveActions.getAll().collect(Collectors.toList()),
            containsInAnyOrder(action1, action2));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.BUY_RI),
            containsInAnyOrder(action1, action2));
    }

    @Test
    public void updateMarketActions() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);
        final Snapshot entityCacheSnapshot = mock(Snapshot.class);

        // Initialize to action1
        liveActions.replaceMarketActions(Stream.of(action1));

        // Remove action1, add action 2 and 3
        liveActions.updateMarketActions(Collections.singleton(action1.getId()),
            Arrays.asList(action2, action3), entityCacheSnapshot);

        verify(entitiesCache).update(entityCacheSnapshot);
        assertThat(liveActions.getAll().collect(Collectors.toList()),
            containsInAnyOrder(action2, action3));
    }

    @Test
    public void testSize() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));
        assertThat(liveActions.size(), is(2));
    }

    @Test
    public void testGet() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));

        assertThat(liveActions.get(action1.getId()).get(), is(action1));
        assertThat(liveActions.get(action2.getId()).get(), is(action2));
        assertFalse(liveActions.get(action1.getId() + 100).isPresent());
    }

    @Test
    public void testMultiGet() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);

        liveActions.replaceMarketActions(Stream.of(action1, action2));
        liveActions.replaceRiActions(Stream.of(action3));

        // Try to get one from market and one from RI.
        // Shouldn't return the other one from market.
        assertThat(liveActions.get(Sets.newHashSet(action1.getId(), action3.getId())).collect(Collectors.toList()),
            containsInAnyOrder(action1, action3));
    }

    @Test
    public void testGetByFilterNoStartEndDateAllInvolvedEntities() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(action1)).thenReturn(true);
        when(queryFilter.test(action2)).thenReturn(false);

        // Target all entities.
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();

        when(queryFilterFactory.newQueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE)).thenReturn(queryFilter);

        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()), containsInAnyOrder(action1));
    }

    @Test
    public void testGetByFilterNoStartEndDateLimitInvolvedEntities() throws UnsupportedActionException {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(100, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(200, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        final QueryFilter queryFilter = mock(QueryFilter.class);
        // Suppose both actions would have passed the query filter.
        when(queryFilter.test(action1)).thenReturn(true);
        when(queryFilter.test(action2)).thenReturn(true);

        // Target only action 1's entity.
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addOids(ActionDTOUtil.getPrimaryEntity(action1.getRecommendation()).getId()))
            .build();

        when(queryFilterFactory.newQueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE)).thenReturn(queryFilter);

        // We should only get action1 - action 2 shouldn't even be considered by the query filter.
        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()), containsInAnyOrder(action1));
        verify(queryFilter, never()).test(action2);
    }

    @Test
    public void testGetByFilterWithStartEndDateAllInvolvedEntities() {
        final Instant start = clock.instant().minusMillis(100);
        final Instant end = clock.instant().plusMillis(100);
        final LocalDateTime startDate = LocalDateTime.ofInstant(start, clock.getZone());
        final LocalDateTime endDate = LocalDateTime.ofInstant(end, clock.getZone());

        // This is a historical action that matches the filter
        final Action historicalAction = ActionOrchestratorTestUtils.createMoveAction(100, 2);

        // This is a historical action that doesn't match the filter
        final Action badHistoricalAction = ActionOrchestratorTestUtils.createMoveAction(101, 2);

        // This is a succeeded action in the live action store.
        final Action succeededAction = spy(ActionOrchestratorTestUtils.createMoveAction(201, 2));
        doReturn(ActionState.SUCCEEDED).when(succeededAction).getState();

        // This is a failed action in the live action store.
        final Action failedAction = spy(ActionOrchestratorTestUtils.createMoveAction(202, 2));
        doReturn(ActionState.FAILED).when(failedAction).getState();

        // This is a live action with a recommendation time after the end time.
        final Action lateRecommendedAction = spy(ActionOrchestratorTestUtils.createMoveAction(203, 2));
        doReturn(LocalDateTime.ofInstant(end.plusMillis(100), clock.getZone()))
            .when(lateRecommendedAction).getRecommendationTime();

        // This is a live action within the time range that matches the filter.
        final Action liveAction = spy(ActionOrchestratorTestUtils.createMoveAction(204, 2));
        // This is a live action within the time range that matches the filter.
        doReturn(startDate).when(liveAction).getRecommendationTime();

        // This is a live action within the time range that doesn't match the filter.
        final Action badLiveAction = spy(ActionOrchestratorTestUtils.createMoveAction(205, 2));
        doReturn(startDate).when(badLiveAction).getRecommendationTime();

        liveActions.replaceMarketActions(Stream.of(succeededAction, failedAction,
            lateRecommendedAction, liveAction, badLiveAction));

        when(actionHistoryDao.getActionHistoryByDate(startDate, endDate))
            .thenReturn(Arrays.asList(historicalAction, badHistoricalAction));

        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
            .setStartDate(start.toEpochMilli())
            .setEndDate(end.toEpochMilli())
            .build();

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(liveAction)).thenReturn(true);
        when(queryFilter.test(historicalAction)).thenReturn(true);
        when(queryFilter.test(badLiveAction)).thenReturn(false);
        when(queryFilter.test(badHistoricalAction)).thenReturn(false);
        // The succeeded, failed, and late actions shouldn't reach the query filter.

        when(queryFilterFactory.newQueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE)).thenReturn(queryFilter);

        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()),
            containsInAnyOrder(liveAction, historicalAction));
        verify(queryFilter, never()).test(succeededAction);
        verify(queryFilter, never()).test(failedAction);
        verify(queryFilter, never()).test(lateRecommendedAction);
    }

    @Test
    public void testGetByFilterWithStartEndDateLimitedInvolvedEntities() throws UnsupportedActionException {
        final Instant start = clock.instant().minusMillis(100);
        final Instant end = clock.instant().plusMillis(100);
        final LocalDateTime startDate = LocalDateTime.ofInstant(start, clock.getZone());
        final LocalDateTime endDate = LocalDateTime.ofInstant(end, clock.getZone());

        // This is a historical action that matches the filter
        final Action historicalAction = ActionOrchestratorTestUtils.createMoveAction(100, 2);

        // This is a historical action that doesn't match the filter
        final Action badHistoricalAction = ActionOrchestratorTestUtils.createMoveAction(101, 2);

        // This is a live action within the time range that matches the filter
        // on one of the involved entities.
        final Action liveAction = spy(ActionOrchestratorTestUtils.createMoveAction(204, 2));
        // This is a live action within the time range that matches the filter.
        doReturn(startDate).when(liveAction).getRecommendationTime();

        // This is a live action within the time range that doesn't involve one of
        // the target involved entities.
        final Action noInvolvedEntitiesLiveAction = spy(ActionOrchestratorTestUtils.createMoveAction(205, 2));
        doReturn(startDate).when(noInvolvedEntitiesLiveAction).getRecommendationTime();

        liveActions.replaceMarketActions(Stream.of(liveAction, noInvolvedEntitiesLiveAction));

        when(actionHistoryDao.getActionHistoryByDate(startDate, endDate))
            .thenReturn(Arrays.asList(historicalAction, badHistoricalAction));

        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
            .setStartDate(start.toEpochMilli())
            .setEndDate(end.toEpochMilli())
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(ActionDTOUtil.getInvolvedEntityIds(liveAction.getRecommendation()))
                .addAllOids(ActionDTOUtil.getInvolvedEntityIds(historicalAction.getRecommendation())))
            .build();

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(liveAction)).thenReturn(true);
        when(queryFilter.test(historicalAction)).thenReturn(true);
        when(queryFilter.test(badHistoricalAction)).thenReturn(false);
        // The entity with no desired involved entities shouldn't be considered by the filter.

        when(queryFilterFactory.newQueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE)).thenReturn(queryFilter);

        List<ActionView> results = liveActions.get(actionQueryFilter).collect(Collectors.toList());

        assertThat(results, containsInAnyOrder(liveAction, historicalAction));
        verify(queryFilter, never()).test(noInvolvedEntitiesLiveAction);
    }

    @Test
    public void testGetByEntity() throws UnsupportedActionException {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(100, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(200, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        ActionDTOUtil.getInvolvedEntityIds(action1.getRecommendation())
            .forEach(involvedEntityId -> {
                assertThat(liveActions.getByEntity(Collections.singleton(involvedEntityId)).findFirst().get(), is(action1));
            });
        ActionDTOUtil.getInvolvedEntityIds(action2.getRecommendation())
            .forEach(involvedEntityId -> {
                assertThat(liveActions.getByEntity(Collections.singleton(involvedEntityId)).findFirst().get(), is(action2));
            });

        assertThat(liveActions.getByEntity(Arrays.asList(
            ActionDTOUtil.getPrimaryEntityId(action1.getRecommendation()),
            ActionDTOUtil.getPrimaryEntityId(action2.getRecommendation())))
                .collect(Collectors.toList()), containsInAnyOrder(action1, action2));

    }

    @Test
    public void isEmpty() {
        assertTrue(liveActions.isEmpty());
    }

    @Test
    public void getAction() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));

        assertThat(liveActions.getAction(action1.getId()).get(), is(action1));
        assertThat(liveActions.getAction(action2.getId()).get(), is(action2));
        assertFalse(liveActions.getAction(action1.getId() + 100).isPresent());
    }

    @Test
    public void getActionsByPlanType() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));

        assertThat(liveActions.getActionsByPlanType().keySet(),
            containsInAnyOrder(ActionPlanType.MARKET, ActionPlanType.BUY_RI));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.MARKET), containsInAnyOrder(action1));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.BUY_RI), containsInAnyOrder(action2));
    }
}