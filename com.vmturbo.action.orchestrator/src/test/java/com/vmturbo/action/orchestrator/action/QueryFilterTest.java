package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the {@link QueryFilter} class.
 */
public class QueryFilterTest {
    private ActionTranslator actionTranslator = mock(ActionTranslator.class);
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
    private static final long ACTION_PLAN_ID = 9876;

    private final ActionStore actionStore = mock(ActionStore.class);

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

    @Before
    public void setup() {
        when(actionTranslator.translate(any(ActionView.class))).thenReturn(true);
    }

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
                .setInfo(TestActionBuilder.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator);
        assertTrue(new QueryFilter(Optional.of(ActionQueryFilter.newBuilder()
                .addCategories(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build())).test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testFilterActionViewsByCategoryNoMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(TestActionBuilder.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator);
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
    public void testEnvironmentFilterMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(7)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD))))
            .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();
        final ActionView actionView = mock(ActionView.class);
        when(actionView.getRecommendation()).thenReturn(action);
        assertTrue(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testEnvironmentFilterNoMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(7)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD))))
            .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();
        final ActionView actionView = mock(ActionView.class);
        when(actionView.getRecommendation()).thenReturn(action);
        assertFalse(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testEnvironmentFilterUnset() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(7)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.CLOUD))))
            .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .build();
        final ActionView actionView = mock(ActionView.class);
        when(actionView.getRecommendation()).thenReturn(action);
        // Should pass the filter, since no environment type is specified.
        assertTrue(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    @Test
    public void testEnvironmentFilterUnsupportedAction() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            // No "action type".
            .setInfo(ActionInfo.getDefaultInstance())
            .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();
        final ActionView actionView = mock(ActionView.class);
        when(actionView.getRecommendation()).thenReturn(action);
        assertFalse(new QueryFilter(Optional.of(filter))
            .test(actionView, PlanActionStore.VISIBILITY_PREDICATE));
    }

    private ActionView executableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator));
    }

    private ActionView notExecutableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(id)
            .setImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setExplanation(Explanation.newBuilder().build())
            .setExecutable(false)
            .setInfo(TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
            .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator));
    }
}
