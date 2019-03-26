package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Unit Tests for EntitySeverityCache.
 */
public class EntitySeverityCacheTest {

    private final QueryFilter queryFilter = QueryFilter.VISIBILITY_FILTER;
    private final EntitySeverityCache entitySeverityCache = new EntitySeverityCache(queryFilter);
    private final ActionStore actionStore = mock(ActionStore.class);
    private ActionTranslator actionTranslator = mock(ActionTranslator.class);
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
    private final ActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private static final long DEFAULT_SOURCE_ID = 1;
    private static final long ACTION_PLAN_ID = 9876;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        when(actionStore.getVisibilityPredicate()).thenReturn(LiveActionStore.VISIBILITY_PREDICATE);
    }

    @Test
    public void testRefreshVisibleNotDisabled() {
        ActionView action = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL));
        when(actionStore.getActionViews())
            .thenReturn(Collections.singletonMap(action.getRecommendation().getId(), action));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshNotVisibleDisabled() {
        ActionView action = spy(actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.CRITICAL)));
        doReturn(ActionMode.DISABLED).when(action).getMode();

        final Map<Long, ActionView> actionViews = ImmutableMap.of(action.getRecommendation().getId(), action);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        entitySeverityCache.refresh(actionStore);
        assertEquals(Optional.empty(), entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID));
    }

    @Test
    public void testRefreshPicksMaxSeverity() {
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = actionView(executableMove(3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3));

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshIndividualAction() {
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 1, 2, 1, Severity.MINOR));
        ActionView action2 = spy(actionView(executableMove(3, DEFAULT_SOURCE_ID, 1, 4, 1, Severity.CRITICAL)));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 1, 6, 1, Severity.MAJOR));
        ActionView unrelatedAction = actionView(executableMove(5, 999, 1, 6, 1, Severity.CRITICAL));

        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3,
            unrelatedAction.getRecommendation().getId(), unrelatedAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getActionView(Mockito.anyLong()))
            .thenAnswer(new Answer<Optional<ActionView>>() {
                @Override
                public Optional<ActionView> answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    return Optional.of(actionViews.get(args[0]));
                }
            });

        entitySeverityCache.refresh(actionStore);
        assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());

        // Simulate changing the state of the critical action to IN_PROGRESS and refreshing for that action.
        // State should the next most critical action that applies to the entity.
        when(action2.getState()).thenReturn(ActionState.IN_PROGRESS);

        entitySeverityCache.refresh(action2.getRecommendation(), actionStore);
        assertEquals(Severity.MAJOR, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testSeverityCounts() {
        final long sourceId1 = 111L;
        final long sourceId2 = 222L;
        final long sourceId3 = 333L;
        final long missingId = 444L;

        ActionView action1 = actionView(executableMove(0, sourceId1, 1, 2, 1, Severity.MINOR));
        ActionView action2 = actionView(executableMove(3, sourceId2, 1, 4, 1, Severity.MINOR));
        ActionView action3 = actionView(executableMove(5, sourceId3, 1, 6, 1, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3));

        entitySeverityCache.refresh(actionStore);
        final Map<Optional<Severity>, Long> severityCounts =
            entitySeverityCache.getSeverityCounts(Arrays.asList(sourceId1, sourceId2, sourceId3, missingId));
        assertEquals(2L, (long)severityCounts.get(Optional.of(Severity.MINOR)));
        assertEquals(1L, (long)severityCounts.get(Optional.of(Severity.MAJOR)));
        // No actions should map to critical so it should be empty.
        assertTrue(severityCounts.get(Optional.of(Severity.CRITICAL)) == null);
        assertEquals(1L, (long) severityCounts.get(Optional.<Severity>empty())); // The missing ID should map to null
    }

    @Nonnull
    private ActionView actionView(ActionDTO.Action recommendation) {
        return actionFactory.newAction(recommendation, ACTION_PLAN_ID);
    }

    private static ActionDTO.Action executableMove(final long targetId,
                    final long sourceId, int sourceType,
                    final long destinationId, int destinationType,
                    Severity severity) {
        return ActionDTO.Action.newBuilder()
            .setExecutable(true)
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .setId(IdentityGenerator.next())
            .setImportance(mapSeverityToImportance(severity))
            .setInfo(
                    TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType,
                    destinationId, destinationType))
            .setExplanation(Explanation.newBuilder().build())
            .build();
    }

    private static ActionDTO.Action notExecutableMove(final long targetId,
                                                      final long sourceId, final int sourceType,
                                                      final long destinationId, final int destinationType,
                                                      Severity severity) {
        return executableMove(targetId, sourceId, sourceType, destinationId, destinationType, severity)
            .toBuilder()
            .setExecutable(false)
            .build();
    }

    private static double mapSeverityToImportance(Severity severity) {
        switch (severity) {
            case NORMAL:
                return ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD - 1.0;
            case MINOR:
                return ActionDTOUtil.MINOR_SEVERITY_THRESHOLD - 1.0;
            case MAJOR:
                return ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD - 1.0;
            case CRITICAL:
                return ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD + 1.0;
            default:
                throw new IllegalArgumentException("Unknown severity " + severity);
        }
    }
}
