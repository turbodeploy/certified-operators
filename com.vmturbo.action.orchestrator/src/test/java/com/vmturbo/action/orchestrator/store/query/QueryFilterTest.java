package com.vmturbo.action.orchestrator.store.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCostType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDisruptiveness;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionReversibility;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSavingsAmountRangeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.ResourceGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Tests for the {@link QueryFilter} class.
 */
@RunWith(JUnitParamsRunner.class)
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

    // For action categorization the threshold for what is a significant cost is 1e-10
    // Because we use floating point calculations to subtract discounts from costs, there is always
    // the chance for infinitesimal errors to creep into our costs and savings. For this reason we
    // always want to treat very small numbers as effectively zero.
    public static final double SIGNIFICANT = 5e-10;
    public static final double TRIVIAL = SIGNIFICANT / 10.0;

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
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator, 2244L);
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
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator, 0L);

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
        final ActionView actionView = new Action(action, ACTION_PLAN_ID, actionModeCalculator, 2244L);
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

    /**
     * Test query filter for actions with and without associated schedule.
     */
    @Test
    public void testAssociatedScheduleMatch() {
        final long requestedScheduleId = 9L;
        final long otherScheduleId = 10L;
        final ActionQueryFilter actionQueryFilter =
                ActionQueryFilter.newBuilder().setAssociatedScheduleId(requestedScheduleId).build();
        final ActionView actionWithRequestedSchedule = executableMoveAction(221L, 1L, 1, 2L, 1, 3L);
        final ActionView actionWithOtherSchedule = executableMoveAction(222L, 1L, 1, 2L, 1, 3L);
        final ActionView actionWithoutSchedule = executableMoveAction(223L, 1L, 1, 2L, 1, 3L);

        Mockito.when(actionWithRequestedSchedule.getSchedule())
                .thenReturn(Optional.of(
                        new ActionSchedule(1L, 2L, "America/Chicago", requestedScheduleId,
                                "testSchedule", ActionMode.MANUAL, "admin")));
        Mockito.when(actionWithOtherSchedule.getSchedule())
                .thenReturn(Optional.of(
                        new ActionSchedule(1L, 2L, "America/Chicago", otherScheduleId,
                                "testSchedule", ActionMode.MANUAL, "admin")));
        Mockito.when(actionWithoutSchedule.getSchedule()).thenReturn(Optional.empty());

        Assert.assertTrue(
                new QueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE).test(
                        actionWithRequestedSchedule));
        Assert.assertFalse(
                new QueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE).test(
                        actionWithOtherSchedule));
        Assert.assertFalse(
                new QueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE).test(
                        actionWithoutSchedule));
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

    @Test
    public void testAccountFilter() {
        final ActionView actionView =
            executableDeleteAction(0L/*id*/, 3L/*targetId*/, ActionState.SUCCEEDED/*actionState*/);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                        .addAllStates(ImmutableList.of(ActionState.SUCCEEDED, ActionState.FAILED))
                        .setAccountFilter(AccountFilter.newBuilder().addAccountId(2L)
                                          .build())
                        .build();

        assertTrue(new QueryFilter(filter, LiveActionStore.VISIBILITY_PREDICATE)
                                    .test(actionView));
    }

    @Test
    public void testResourceGroupFilter() {
        final ActionView actionView =
                executableDeleteAction(0L/*id*/, 3L/*targetId*/, ActionState.FAILED/*actionState*/);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .addAllStates(ImmutableList.of(ActionState.SUCCEEDED, ActionState.FAILED))
                .setResourceGroupFilter(ResourceGroupFilter.newBuilder().addResourceGroupOid(9L)
                        .build())
                .build();

        assertTrue(new QueryFilter(filter, LiveActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }

    @Test
    public void testInvolvedEntitiesMatchWithOrganizationalScopeFilter() {
        final ActionView actionViewInvolved =
                executableMoveAction(0L/*id*/, 1L/*srcId*/, 1/*srcType*/, 2L/*destId*/, 1/*destType*/, 3L/*targetId*/);
        final List<Long> involvedEntities = Arrays.asList(1L, 5L);

        final ActionView actionViewDeleted =
                executableDeleteAction(7L/*id*/, 3L/*targetId*/, ActionState.SUCCEEDED/*actionState*/);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setInvolvedEntities(InvolvedEntities.newBuilder()
                        .addAllOids(involvedEntities).build())
                .setAccountFilter(AccountFilter.newBuilder().addAccountId(2L)
                        .build())
                .build();

        // Set up combined entitiesRestriction
        Set<Long> expectedEntitiesRestriction = ImmutableSet.of(1L, 3L, 5L, 7L);
        assertTrue(new QueryFilter(filter, LiveActionStore.VISIBILITY_PREDICATE, expectedEntitiesRestriction,
                InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES)
                .test(actionViewInvolved));
        assertTrue(new QueryFilter(filter, LiveActionStore.VISIBILITY_PREDICATE, expectedEntitiesRestriction,
                InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES)
                .test(actionViewDeleted));
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
    @Parameters({
            "CLOUD, CLOUD, true",
            "CLOUD, HYBRID, true",
            "CLOUD, ON_PREM, false",
            "CLOUD, UNKNOWN_ENV, false",
            "HYBRID, CLOUD, true",
            "HYBRID, HYBRID, true",
            "HYBRID, ON_PREM, true",
            "HYBRID, UNKNOWN_ENV, true",
            "ON_PREM, CLOUD, false",
            "ON_PREM, HYBRID, true",
            "ON_PREM, ON_PREM, true",
            // For some reason ON_PREM is the default type when given action type UNKNOWN_ENV
            "ON_PREM, UNKNOWN_ENV, true",
            "UNKNOWN_ENV, CLOUD, false",
            "UNKNOWN_ENV, HYBRID, false",
            "UNKNOWN_ENV, ON_PREM, false",
            "UNKNOWN_ENV, UNKNOWN_ENV, false"
    })
    @TestCaseName("{method} environmentType={0} actionType={1}")
    public void testEnvironmentTypeFiltering(String environmentType, String actionType,
            boolean expectedResult) {
        final ActionQueryFilter filter = ActionQueryFilter.newBuilder().setEnvironmentType(
                EnvironmentType.valueOf(environmentType)).build();

        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(
                                Activate.newBuilder().setTarget(ActionEntity.newBuilder()
                                        .setId(1L)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .setEnvironmentType(EnvironmentType.valueOf(actionType)))))
                .build();

        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);

        assertEquals(String.format("Expected %b for action type %s with environment type filter %s",
                        expectedResult, actionType, environmentType),
                new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE).test(actionView),
                expectedResult);
    }

    /**
     * Test query filter should return true when filter costType is SAVINGS and amount is
     * non-negative or very close to zero
     *
     * Because we use floating point calculations to subtract discounts from costs, there is always
     * the chance for infinitesimal errors to creep into our costs and savings. For this reason we
     * always want to treat very small numbers as effectively zero.
     */
    @Test
    public void testCostTypeFilterMatchForSaving() {
        // all actions with savings near or above zero should return true
        assertTrue(testActionWithSavingsForCostType(SIGNIFICANT, ActionCostType.SAVINGS));
        assertTrue(testActionWithSavingsForCostType(TRIVIAL, ActionCostType.SAVINGS));
        assertTrue(testActionWithSavingsForCostType(-1 * TRIVIAL, ActionCostType.SAVINGS));

        // any action with significant negative savigns should return false
        assertFalse(testActionWithSavingsForCostType(-1 * SIGNIFICANT, ActionCostType.SAVINGS));
    }

    /**
     * Test query filter should return true when filter costType is INVESTMENT and amount is
     * below zero by more than a trivial amount
     *
     * Because we use floating point calculations to subtract discounts from costs, there is always
     * the chance for infinitesimal errors to creep into our costs and savings. For this reason we
     * always want to treat very small numbers as effectively zero.
     */
    @Test
    public void testCostTypeFilterMatchForInvestment() {
        // only actions with significant negative savings should return true
        assertTrue(testActionWithSavingsForCostType(-1 * SIGNIFICANT, ActionCostType.INVESTMENT));

        // all actions with savings near or above zero should return false
        assertFalse(testActionWithSavingsForCostType(-1 * TRIVIAL, ActionCostType.INVESTMENT));
        assertFalse(testActionWithSavingsForCostType(TRIVIAL, ActionCostType.INVESTMENT));
        assertFalse(testActionWithSavingsForCostType(SIGNIFICANT, ActionCostType.INVESTMENT));
    }

    /**
     * Test query filter should return true when filter costType is SAVINGS or ACTION_COST_TYPE_NONE
     * and amount is very close to zero
     *
     * Because we use floating point calculations to subtract discounts from costs, there is always
     * the chance for infinitesimal errors to creep into our costs and savings. For this reason we
     * always want to treat very small numbers as effectively zero.
     */
    @Test
    public void testCostTypeFilterMatchForActionCostTypeNone() {
        // only actions with savings close to zero should return true
        assertTrue(testActionWithSavingsForCostType(TRIVIAL, ActionCostType.ACTION_COST_TYPE_NONE));
        assertTrue(testActionWithSavingsForCostType(-1 * TRIVIAL, ActionCostType.ACTION_COST_TYPE_NONE));

        // actions with significant positive or negative savings should return false
        assertFalse(testActionWithSavingsForCostType(SIGNIFICANT, ActionCostType.ACTION_COST_TYPE_NONE));
        assertFalse(testActionWithSavingsForCostType(-1 * SIGNIFICANT, ActionCostType.ACTION_COST_TYPE_NONE));
    }

    /**
     * Test query filter should return true when filter descriptionQuery matches action description.
     */
    @Test
    public void testDescriptionQueryFilterMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .build();

        // match 1
        ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setDescriptionQuery("(.*template1.*)|(.*template2.*)")
                .build();
        ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);

        when(actionView.getDescription()).thenReturn("Scale vm from template1 to template2");
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 2
        filter = ActionQueryFilter.newBuilder()
                .setDescriptionQuery(".*template1.*")
                .build();
        when(actionView.getDescription()).thenReturn("Scale vm from template1 to template2");
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 3
        filter = ActionQueryFilter.newBuilder()
                .setDescriptionQuery("template1")
                .build();
        when(actionView.getDescription()).thenReturn("Scale vm from template1 to template2");
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 4
        filter = ActionQueryFilter.newBuilder()
                .setDescriptionQuery("^.*" + "t2\\.nano" + ".*$")
                .build();
        when(actionView.getDescription()).thenReturn("Scale vm from template1 to t2.nano");
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 5
        filter = ActionQueryFilter.newBuilder()
                .setDescriptionQuery("^.*\\btemplate2\\b.*$")
                .build();
        when(actionView.getDescription()).thenReturn("Scale vm from template1 to template2");
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }

    /**
     * Test query filter should return true when filter riskQuery matches action related risks.
     */
    @Test
    public void testRiskQueryFilterMatch() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .build();

        ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        when(actionView.getCombinedRisksString()).thenReturn("IOPS Congestion, VMem Congestion, Underutilized VMem, Underutilized IOPS");

        // match 1
        ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("(.*IOPS Congestion.*)|(.*VMem Congestion.*)|(.*Underutilized VMem.*)|(.*Underutilized IOPS.*)")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 2
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery(".*IOPS Congestion.*")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 3
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery(".*VMem Congestion.*")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 4
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("Underutilized VMem")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 5
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("Underutilized IOPS")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 6
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("^.*\\bIOPS\\b.*$")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 7
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("^.*\\biops\\b.*$")
                .build();
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 8
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("^(?!.*(\\bIOPS Congestion\\b|\\bVMem Congestion\\\\b)).*(\\bUUnderutilized VMem\\b).*$")
                .build();
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        // match 9
        filter = ActionQueryFilter.newBuilder()
                .setRiskQuery("(\\bIOPS Congestion\\b|\\bUnderutilized VMem\\b|\\bUnderutilized IOPS\\b)")
                .build();
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }

    /**
     * Test query filter should return true when filter disruptiveness matches action disruptivness.
     */
    @Test
    public void testDisruptivenessFilter() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setDisruptive(true)
                .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setDisruptiveness(ActionDisruptiveness.DISRUPTIVE)
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        final ActionDTO.Action action2 = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setDisruptive(false)
                .build();
        final ActionQueryFilter filter2 = ActionQueryFilter.newBuilder()
                .setDisruptiveness(ActionDisruptiveness.NON_DISRUPTIVE)
                .build();
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(action2);
        assertTrue(new QueryFilter(filter2, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView2));
    }

    /**
     * Test query filter should return true when filter reversibility matches action reversibility.
     */
    @Test
    public void testReversibilityFilter() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setReversible(true)
                .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setReversibility(ActionReversibility.REVERSIBLE)
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        final ActionDTO.Action action2 = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setReversible(false)
                .build();
        final ActionQueryFilter filter2 = ActionQueryFilter.newBuilder()
                .setReversibility(ActionReversibility.IRREVERSIBLE)
                .build();
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(action2);
        assertTrue(new QueryFilter(filter2, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView2));
    }

    /**
     * Test query filter should return true when filter savingsAmountRange matches action savingsAmountRange.
     */
    @Test
    public void testSavingsAmountRangeFilter() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setSavingsPerHour(CurrencyAmount.newBuilder()
                        .setAmount(15.0f))
                .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setSavingsAmountRange(ActionSavingsAmountRangeFilter.newBuilder()
                        .setMinValue(10.0f)
                        .setMaxValue(20.0f)
                )
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));


        final ActionDTO.Action action2 = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .setSavingsPerHour(CurrencyAmount.newBuilder()
                        .setAmount(22.0f))
                .build();
        final ActionQueryFilter filter2 = ActionQueryFilter.newBuilder()
                .setSavingsAmountRange(ActionSavingsAmountRangeFilter.newBuilder()
                        .setMinValue(10.0f)
                        .setMaxValue(20.0f)
                )
                .build();
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(action2);
        assertFalse(new QueryFilter(filter2, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView2));
    }

    /**
     * Test query filter should return true when filter hasSchedule matches action having schedule.
     */
    @Test
    public void testHasScheduleFilter() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setHasSchedule(true)
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        final ActionSchedule actionSchedule = new ActionSchedule(1L, 2L, "America/Chicago", 12L, "testSchedule",
                ActionMode.MANUAL, "admin");
        when(actionView.getSchedule()).thenReturn(Optional.of(actionSchedule));
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        final ActionQueryFilter filter2 = ActionQueryFilter.newBuilder()
                .setHasSchedule(false)
                .build();
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(action);
        when(actionView2.getSchedule()).thenReturn(Optional.empty());
        assertTrue(new QueryFilter(filter2, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView2));
    }

    /**
     * Test query filter should return true when filter hasPrerequisites matches action having prerequisites.
     */
    @Test
    public void testHasPrerequisitesFilter() {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .addPrerequisite(Prerequisite.newBuilder())
                .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .setHasPrerequisites(true)
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);
        assertTrue(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));

        final ActionDTO.Action action2 = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.getDefaultInstance())
                .build();
        final ActionQueryFilter filter2 = ActionQueryFilter.newBuilder()
                .setHasPrerequisites(false)
                .build();
        final ActionView actionView2 = ActionOrchestratorTestUtils.mockActionView(action2);
        assertTrue(new QueryFilter(filter2, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView2));
    }

    /**
     * Test that when {@link ActionView#getAssociatedCloudServiceProviderId()} returns {@link Optional#empty()},
     * {@link QueryFilter#test(ActionView)} returns false.
     */
    @Test
    public void testQueryFilterRelatedCspNoMatch() {
       final ActionDTO.Action scaleAction = ActionDTO.Action.newBuilder()
           .setId(11111L)
           .setInfo(ActionInfo.newBuilder()
               .build())
           .setDeprecatedImportance(0)
           .setExplanation(Explanation.newBuilder()
               .build())
           .build();
       final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(scaleAction);
       when(actionView.getAssociatedCloudServiceProviderId()).thenReturn(Optional.empty());
       final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
           .addRelatedCloudServiceProviderIds(12345L)
           .build();
       Assert.assertFalse(new QueryFilter(actionQueryFilter, PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    /**
     * Test that when {@link ActionView#getAssociatedCloudServiceProviderId()} returns a Cloud Service Provider id that
     * matches with the one specified in the {@link ActionQueryFilter} instance, then
     * {@link QueryFilter#test(ActionView)} returns true.
     */
    @Test
    public void testQueryFilterRelatedCspMatch() {
        final ActionDTO.Action scaleAction = ActionDTO.Action.newBuilder()
            .setId(11111L)
            .setInfo(ActionInfo.newBuilder()
                .build())
            .setDeprecatedImportance(0)
            .setExplanation(Explanation.newBuilder()
                .build())
            .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(scaleAction);
        final long cspId = 12345L;
        when(actionView.getAssociatedCloudServiceProviderId()).thenReturn(Optional.of(cspId));
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
            .addRelatedCloudServiceProviderIds(cspId)
            .build();
        Assert.assertTrue(new QueryFilter(actionQueryFilter, PlanActionStore.VISIBILITY_PREDICATE).test(actionView));
    }

    private ActionView executableMoveAction(long id, long sourceId, int sourceType, long destId, int destType, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(id)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator, 2244L));
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

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator, 2244L));
    }

    private ActionView executableDeleteAction(long id, long targetId, ActionState actionState) {
            final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                    .setId(id)
                    .setDeprecatedImportance(0)
                    .setExecutable(true)
                    .setExplanation(Explanation.newBuilder().build())
                    .setInfo(TestActionBuilder.makeDeleteInfo(targetId))
                    .build();

        return spy(new Action(new SerializationState(
                ACTION_PLAN_ID,
                action,
                null,
                ActionDecision.newBuilder().build(),
                null,
                actionState,
                new ActionTranslation(action),
                2L,
                9L,
                null,
                id), actionModeCalculator));
    }

    private boolean testActionWithSavingsForCostType(double nonZero, ActionCostType actionCostType) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                        .setId(1)
                        .setDeprecatedImportance(0)
                        .setExecutable(true)
                        .setExplanation(Explanation.newBuilder().build())
                        .setInfo(ActionInfo.getDefaultInstance())
                        .setSavingsPerHour(CurrencyAmount.newBuilder()
                                                           .setAmount(nonZero))
                        .build();

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                        .setCostType(actionCostType)
                        .build();
        return new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                        .test(ActionOrchestratorTestUtils.mockActionView(action));
    }

    /**
     * Test pod move is filtered out from the VM move
     */
    @Test
    public void testFilterOutPodMoveUnderVmMove() {
        // build a pod move action
        ActionDTO.Move.Builder moveBuilder = ActionDTO.Move.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(0L, EntityType.CONTAINER_POD_VALUE))
                .addChanges(ActionDTO.ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                                .setId(1L)
                                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .build())
                        .setDestination(ActionEntity.newBuilder()
                                .setId(2L)
                                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .build())
                        .build());
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder().setMove(moveBuilder.build()))
                .build();

        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(action);

        // build a filter for VM move
        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
                .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addTypes(ActionType.MOVE)
                .build();

        // expect the pod move is filter out and return false
        assertFalse(new QueryFilter(filter, PlanActionStore.VISIBILITY_PREDICATE)
                .test(actionView));
    }
}
