package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;

/**
 * Tests for the {@link QueryFilter} class.
 */
public class QueryFilterTest {
    private static final long ACTION_PLAN_ID = 9876;

    private final ActionStore actionStore = Mockito.mock(ActionStore.class);

    private final ActionQueryFilter visibleFilter = ActionQueryFilter.newBuilder()
        .setVisible(true)
        .build();

    private final ActionQueryFilter notVisibleFilter = ActionQueryFilter.newBuilder()
        .setVisible(false)
        .build();

    private final ActionQueryFilter allFilter = ActionQueryFilter.newBuilder()
        .build();

    private final ActionQueryFilter succeededFilter = ActionQueryFilter.newBuilder()
            .addStates(ActionState.SUCCEEDED)
            .build();

    private final ActionQueryFilter readyVisibleFilter = ActionQueryFilter.newBuilder()
            .addStates(ActionState.READY)
            .setVisible(true)
            .build();

    @Test
    public void testEmptyFilter() throws Exception {
        final QueryFilter filter = new QueryFilter(Optional.empty());
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        // Even if the visibility is false, the spec should still pass the test if no visibility
        // filter is included.
        assertTrue(filter.test(actionView, view -> false));
        assertTrue(filter.test(executableMoveAction(0L, 4L, 1, 5L, 1, 6L), actionSpec -> false));
    }

    @Test
    public void testExecutableDisabledNotVisible() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        assertFalse(new QueryFilter(Optional.of(visibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertTrue(new QueryFilter(Optional.of(notVisibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualNotExecutableNotVisible() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertFalse(new QueryFilter(Optional.of(visibleFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertTrue(new QueryFilter(Optional.of(notVisibleFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualReadyAndExecutableVisible() throws Exception {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(Optional.of(visibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertFalse(new QueryFilter(Optional.of(notVisibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualVisibleNotSetDoesNotFilter() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(Optional.of(allFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testRecommendVisible() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertTrue(new QueryFilter(Optional.of(allFilter)).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateMatch() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(actionView.getState()).thenReturn(ActionState.SUCCEEDED);

        assertTrue(new QueryFilter(Optional.of(succeededFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateNoMatch() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertFalse(new QueryFilter(Optional.of(succeededFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testTypeFilterNoMatch() throws Exception {
        final ActionView actionView =
                notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        assertFalse(new QueryFilter(Optional.of(ActionQueryFilter.newBuilder()
                .addTypes(ActionType.PROVISION)
                .build())).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testTypeFilterMatch() throws Exception {
        final ActionView actionView =
                notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        assertTrue(new QueryFilter(Optional.of(ActionQueryFilter.newBuilder()
                .addTypes(ActionType.MOVE)
                .build())).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testFilterActionViewsByCategory() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(ActionTest.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID);
        assertTrue(new QueryFilter(Optional.of(ActionQueryFilter.newBuilder()
                .addCategories(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build())).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testFilterActionViewsByCategoryNoMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(ActionTest.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID);
        assertFalse(new QueryFilter(Optional.of(ActionQueryFilter.newBuilder()
                .addCategories(ActionCategory.COMPLIANCE)
                .build())).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateAndVisibleMatch() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        // Using PlanActionStore's visibility predicate, so the
        // action spec should be visible.
        assertTrue(new QueryFilter(Optional.of(readyVisibleFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateAndVisibleNoMatch() throws Exception {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        // Using LiveActionStore's visibility predicate, so the
        // action spec shouldn't be visible.
        assertFalse(new QueryFilter(Optional.of(readyVisibleFilter))
                .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testInvolvedEntitiesNoMatch() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        final List<Long> involvedEntities = Arrays.asList(4L, 5L);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        assertFalse(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testEmptyInvolvedEntities() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        final List<Long> involvedEntities = Collections.emptyList();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        assertFalse(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testInvolvedEntitiesMatch() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        final List<Long> involvedEntities = Arrays.asList(1L, 5L);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        assertTrue(new QueryFilter(Optional.of(filter))
                                    .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testFilterActionViewsByEntity() {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1, 2L, 1, 10L);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 1, 3L, 1, 11L);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 1, 2L, 1, 12L);
        final List<Long> involvedEntities = Arrays.asList(1L, 4L, 6L);

        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            actionView1.getId(), actionView1, actionView2.getId(), actionView2, actionView3.getId(),
            actionView3);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        final Multimap<Long, ActionView> testMap = new QueryFilter(Optional.of(filter))
            .filterActionViewsByEntityId(actionStore);
        assertEquals(2, testMap.keySet().size());
        assertTrue(testMap.containsKey(1L));
        assertEquals(2, testMap.get(1L).size());
        assertEquals(1, testMap.get(4L).size());
    }

    @Test
    public void testFilterActionViewsGroupByDate() {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1, 2L, 1, 10L);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 1, 3L, 1, 11L);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 1, 2L, 1, 12L);
        //final List<Long> involvedEntities = Arrays.asList(1L, 4L, 6L);
        final LocalDateTime startDate = LocalDateTime.now();
        final LocalDateTime endDate = startDate;
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
                actionView1.getId(), actionView1, actionView2.getId(), actionView2, actionView3.getId(),
                actionView3);
        when(actionStore.getActionViewsByDate(any(), any())).thenReturn(actionViews);

        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);


        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                                .setStartDate(11111l)
                                .setEndDate(22222l)
                                .build();

        final Map<Long, List<ActionView>> testMap = new QueryFilter(Optional.of(filter))
                .filteredActionViewsGroupByDate(actionStore);
        // 1 entry due to all actions should be created a the same time
        assertEquals(1, testMap.keySet().size());
        assertEquals(1l, testMap.values().stream().count());
        // values should have three actions
        assertEquals(3, testMap.values().stream().findFirst().get().size());
    }


    @Test
    public void testFilterActionViewsGroupByDateNoMatch() {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1, 2L, 1, 10L);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 1, 3L, 1, 11L);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 1, 2L, 1, 12L);
        //final List<Long> involvedEntities = Arrays.asList(1L, 4L, 6L);
       final Map<Long, ActionView> actionViews = ImmutableMap.of(
                actionView1.getId(), actionView1, actionView2.getId(), actionView2, actionView3.getId(),
                actionView3);
        when(actionStore.getActionViewsByDate(any(), any())).thenReturn(actionViews);

        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);


        final ActionQueryFilter filter = ActionQueryFilter.newBuilder().build();

        final Map<Long, List<ActionView>> testMap = new QueryFilter(Optional.of(filter))
                .filteredActionViewsGroupByDate(actionStore);
        assertTrue(testMap.isEmpty());
    }


    @Test
    public void testFilterActionViewsByEntityNoMatch() {
        final ActionView actionView1 = executableMoveAction(0L, 1L, 1, 2L, 1, 3L);
        final List<Long> involvedEntities = Collections.singletonList(6L);

        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            actionView1.getId(), actionView1);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        final Multimap<Long, ActionView> testMap = new QueryFilter(Optional.of(filter))
            .filterActionViewsByEntityId(actionStore);
        assertTrue(testMap.isEmpty());
    }

    private ActionView executableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionTest.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
                .build();

        return spy(new Action(action, ACTION_PLAN_ID));
    }

    private ActionView notExecutableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                        .setId(id)
                        .setImportance(0)
                        .setExplanation(Explanation.newBuilder().build())
                        .setExecutable(false)
                        .setInfo(ActionTest.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
                        .build();

        return spy(new Action(action, ACTION_PLAN_ID));
    }
}
