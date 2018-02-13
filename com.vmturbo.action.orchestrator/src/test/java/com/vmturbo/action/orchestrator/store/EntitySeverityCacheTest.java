package com.vmturbo.action.orchestrator.store;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.assertj.core.util.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.commons.idgen.IdentityGenerator;

public class EntitySeverityCacheTest {

    private final QueryFilter queryFilter = QueryFilter.VISIBILITY_FILTER;
    private final EntitySeverityCache entitySeverityCache = new EntitySeverityCache(queryFilter);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final ActionFactory actionFactory = new ActionFactory();
    private static final long DEFAULT_SOURCE_ID = 1;
    private static final long ACTION_PLAN_ID = 9876;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        when(actionStore.getVisibilityPredicate()).thenReturn(LiveActionStore.VISIBILITY_PREDICATE);
    }

    @Test
    public void testRefreshVisibleReady() {
        ActionView action = actionView(executableMove(0, DEFAULT_SOURCE_ID, 2, Severity.CRITICAL));
        when(actionStore.getActionViews())
            .thenReturn(Maps.newHashMap(action.getRecommendation().getId(), action));

        entitySeverityCache.refresh(actionStore);
        Assert.assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshVisibleNotReady() {
        ActionView action = spy(actionView(executableMove(0, DEFAULT_SOURCE_ID, 2, Severity.CRITICAL)));
        when(action.getState()).thenReturn(ActionState.CLEARED);

        final Map<Long, ActionView> actionViews = ImmutableMap.of(action.getRecommendation().getId(), action);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        entitySeverityCache.refresh(actionStore);
        Assert.assertEquals(Optional.empty(), entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID));
    }

    @Test
    public void testRefreshNotVisibleReady() {
        ActionView action = actionView(notExecutableMove(0, DEFAULT_SOURCE_ID, 2, Severity.CRITICAL));
        when(actionStore.getActionViews())
            .thenReturn(Maps.newHashMap(action.getRecommendation().getId(), action));

        entitySeverityCache.refresh(actionStore);
        Assert.assertEquals(Optional.empty(), entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID));
    }

    @Test
    public void testRefreshPicksMaxSeverity() {
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 2, Severity.MINOR));
        ActionView action2 = actionView(executableMove(3, DEFAULT_SOURCE_ID, 4, Severity.CRITICAL));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 6, Severity.MAJOR));

        when(actionStore.getActionViews()).thenReturn(ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3));

        entitySeverityCache.refresh(actionStore);
        Assert.assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Test
    public void testRefreshIndividualAction() {
        ActionView action1 = actionView(executableMove(0, DEFAULT_SOURCE_ID, 2, Severity.MINOR));
        ActionView action2 = spy(actionView(executableMove(3, DEFAULT_SOURCE_ID, 4, Severity.CRITICAL)));
        ActionView action3 = actionView(executableMove(5, DEFAULT_SOURCE_ID, 6, Severity.MAJOR));
        ActionView unrelatedAction = actionView(executableMove(5, 999, 6, Severity.CRITICAL));

        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            action1.getRecommendation().getId(), action1,
            action2.getRecommendation().getId(), action2,
            action3.getRecommendation().getId(), action3,
            unrelatedAction.getRecommendation().getId(), unrelatedAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        entitySeverityCache.refresh(actionStore);
        Assert.assertEquals(Severity.CRITICAL, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());

        // Simulate changing the state of the critical action to IN_PROGRESS and refreshing for that action.
        // State should the next most critical action that applies to the entity.
        when(action2.getState()).thenReturn(ActionState.IN_PROGRESS);

        entitySeverityCache.refresh(action2.getRecommendation(), actionStore);
        Assert.assertEquals(Severity.MAJOR, entitySeverityCache.getSeverity(DEFAULT_SOURCE_ID).get());
    }

    @Nonnull
    private ActionView actionView(ActionDTO.Action recommendation) {
        return actionFactory.newAction(recommendation, ACTION_PLAN_ID);
    }

    private static ActionDTO.Action executableMove(final long targetId, final long sourceId, final long destinationId,
                                                   Severity severity) {
        return ActionDTO.Action.newBuilder()
            .setExecutable(true)
            .setId(IdentityGenerator.next())
            .setImportance(mapSeverityToImportance(severity))
            .setInfo(ActionTest.makeMoveInfo(targetId, sourceId, destinationId))
            .setExplanation(Explanation.newBuilder().build())
            .build();
    }

    private static ActionDTO.Action notExecutableMove(final long targetId, final long sourceId,
                                                      final long destinationId, Severity severity) {
        return executableMove(targetId, sourceId, destinationId, severity)
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