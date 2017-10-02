package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;

/**
 * Tests for the {@link QueryFilter} class.
 */
public class QueryFilterTest {
    private static final long ACTION_PLAN_ID = 9876;

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
        final ActionView actionView = notExecutableMoveAction(1L, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        // Even if the visibility is false, the spec should still pass the test if no visibility filter is included.
        assertTrue(filter.test(actionView, view -> false));
        assertTrue(filter.test(executableMoveAction(4L, 5L, 6L), actionSpec -> false));
    }

    @Test
    public void testExecutableDisabledNotVisible() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        assertFalse(new QueryFilter(Optional.of(visibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertTrue(new QueryFilter(Optional.of(notVisibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualNotExecutableNotVisible() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertFalse(new QueryFilter(Optional.of(visibleFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertTrue(new QueryFilter(Optional.of(notVisibleFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualReadyAndExecutableVisible() throws Exception {
        final ActionView actionView = executableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(Optional.of(visibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
        assertFalse(new QueryFilter(Optional.of(notVisibleFilter))
            .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testManualVisibleNotSetDoesNotFilter() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(Optional.of(allFilter)).test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testRecommendVisible() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertTrue(new QueryFilter(Optional.of(allFilter)).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateMatch() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(actionView.getState()).thenReturn(ActionState.SUCCEEDED);

        assertTrue(new QueryFilter(Optional.of(succeededFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateNoMatch() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertFalse(new QueryFilter(Optional.of(succeededFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateAndVisibleMatch() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        // Using PlanActionStore's visibility predicate, so the
        // action spec should be visible.
        assertTrue(new QueryFilter(Optional.of(readyVisibleFilter))
                .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testStateAndVisibleNoMatch() throws Exception {
        final ActionView actionView = notExecutableMoveAction(1l, 2L, 3L);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        // Using LiveActionStore's visibility predicate, so the
        // action spec shouldn't be visible.
        assertFalse(new QueryFilter(Optional.of(readyVisibleFilter))
                .test(actionView, LiveActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testInvolvedEntitiesNoMatch() {
        final ActionView actionView = executableMoveAction(1L, 2L, 3L);
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
        final ActionView actionView = executableMoveAction(1L, 2L, 3L);
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
        final ActionView actionView = executableMoveAction(1L, 2L, 3L);
        final List<Long> involvedEntities = Arrays.asList(1L, 5L);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities).build())
            .build();

        assertTrue(new QueryFilter(Optional.of(filter))
                                    .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    private ActionView executableMoveAction(long sourceId, long destId, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                            .setSourceId(sourceId)
                            .setDestinationId(destId)
                            .setTargetId(targetId))
                ).build();

        return spy(new Action(action, ACTION_PLAN_ID));
    }


    private ActionView notExecutableMoveAction(long sourceId, long destId, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setImportance(0)
                .setExplanation(Explanation.newBuilder().build())
                .setExecutable(false)
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                            .setSourceId(sourceId)
                            .setDestinationId(destId)
                            .setTargetId(targetId))
                ).build();

        return spy(new Action(action, ACTION_PLAN_ID));
    }

}