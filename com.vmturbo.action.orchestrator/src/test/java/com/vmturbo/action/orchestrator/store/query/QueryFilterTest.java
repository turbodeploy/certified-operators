package com.vmturbo.action.orchestrator.store.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for the {@link QueryFilter} class.
 */
public class QueryFilterTest {
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
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
    public void testEmptyFilter() {
        // Even if the visibility is false, the spec should still pass the test if no visibility
        // filter is included.
        final QueryFilter filter = new QueryFilter(ActionQueryFilter.newBuilder().build(), view -> false);
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        assertTrue(filter.test(actionView));
        assertTrue(filter.test(executableMoveAction(0L, 4L, 1, 5L, 1, 6L)));
    }

    @Test
    public void testExecutableDisabledNotVisible() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.DISABLED);

        assertFalse(new QueryFilter(visibleFilter, LiveActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
        assertTrue(new QueryFilter(notVisibleFilter, LiveActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testManualNotExecutableVisible() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(visibleFilter, LiveActionStore.VISIBILITY_PREDICATE).test(actionView));
        assertFalse(new QueryFilter(notVisibleFilter, LiveActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testManualReadyAndExecutableVisible() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(visibleFilter, LiveActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
        assertFalse(new QueryFilter(notVisibleFilter, LiveActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testManualVisibleNotSetDoesNotFilter() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        assertTrue(new QueryFilter(allFilter, LiveActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testRecommendVisible() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertTrue(new QueryFilter(allFilter, PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testStateMatch() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);
        when(actionView.getState()).thenReturn(ActionState.SUCCEEDED);

        assertTrue(new QueryFilter(succeededFilter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }

    @Test
    public void testStateNoMatch() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.RECOMMEND);

        assertFalse(new QueryFilter(succeededFilter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }

    @Test
    public void testTypeFilterNoMatch() {
        final ActionView actionView =
                notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        assertFalse(new QueryFilter(ActionQueryFilter.newBuilder()
                .addTypes(ActionType.PROVISION)
                .build(), PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testTypeFilterMatch() {
        final ActionView actionView =
                notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        assertTrue(new QueryFilter(ActionQueryFilter.newBuilder()
                .addTypes(ActionType.MOVE)
                .build(), PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testFilterActionViewsByCategory() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(TestActionBuilder.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator);
        assertTrue(new QueryFilter(ActionQueryFilter.newBuilder()
                .addCategories(ActionCategory.EFFICIENCY_IMPROVEMENT)
                .build(), PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    /**
     * Tests various filters based on severity.
     */
    @Test
    public void testFilterActionViewsBySeverity() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(TestActionBuilder.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator);

        // no severity filter: should succeed
        assertTrue(new QueryFilter(ActionQueryFilter.getDefaultInstance(),
                                   PlanActionStore.VISIBILITY_PREDICATE)
                        .test(actionView));

        // only critical actions accepted: should fail (this is a minor severity action)
        assertFalse(new QueryFilter(ActionQueryFilter.newBuilder()
                                        .addSeverities(Severity.CRITICAL)
                                        .build(),
                                    PlanActionStore.VISIBILITY_PREDICATE)
                        .test(actionView));

        // only minor severity actions accepted: should succeed
        assertTrue(new QueryFilter(ActionQueryFilter.newBuilder()
                                        .addSeverities(Severity.MINOR)
                                        .build(),
                                   PlanActionStore.VISIBILITY_PREDICATE)
                        .test(actionView));

        // only minor and major severity actions accepted: should succeed
        assertTrue(new QueryFilter(ActionQueryFilter.newBuilder()
                                        .addSeverities(Severity.MAJOR)
                                        .addSeverities(Severity.MINOR)
                                        .build(),
                                   PlanActionStore.VISIBILITY_PREDICATE)
                        .test(actionView));
    }

    @Test
    public void testFilterActionViewsByCategoryNoMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(0L)
                .setInfo(TestActionBuilder.makeMoveInfo(3L, 1L, 1, 2L, 1))
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.getDefaultInstance()))))
                .build();
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator);
        assertFalse(new QueryFilter(ActionQueryFilter.newBuilder()
                .addCategories(ActionCategory.COMPLIANCE)
                .build(), PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    @Test
    public void testStateAndVisibleMatch() {
        final ActionView actionView =
            notExecutableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        when(actionView.getMode()).thenReturn(ActionMode.MANUAL);

        // Using PlanActionStore's visibility predicate, so the
        // action spec should be visible.
        assertTrue(new QueryFilter(readyVisibleFilter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // Using LiveActionStore's visibility predicate, so the
        // action spec should be visible.
        assertTrue(new QueryFilter(readyVisibleFilter, LiveActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
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

        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
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

        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
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

        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                                    .test(actionView));
    }

    /**
     * Test that the query filter passes an action that doesn't match the involved entity type
     * filter.
     */
    @Test
    public void testInvolvedEntityTypeMatch() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .addEntityType(1)
            .build();

        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    /**
     * Test that the query filter discards an action that doesn't match the involved entity type
     * filter.
     */
    @Test
    public void testInvolvedEntityTypeNoMatch() {
        final ActionView actionView =
            executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .addEntityType(2)
            .build();

        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testEnvironmentFilterMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
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
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testEnvironmentFilterNoMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
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
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testEnvironmentFilterUnset() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
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
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        // Should pass the filter, since no environment type is specified.
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    @Test
    public void testEnvironmentFilterUnsupportedAction() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            // No "action type".
            .setInfo(ActionInfo.getDefaultInstance())
            .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
            .test(actionView));
    }

    private ActionView executableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator));
    }

    private ActionView notExecutableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(id)
            .setDeprecatedImportance(0)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setExplanation(Explanation.newBuilder().build())
            .setExecutable(false)
            .setInfo(TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
            .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator));
    }
}
