package com.vmturbo.action.orchestrator.execution.affected.entities;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Verifies the expected use cases of affected entities manager.
 */
@RunWith(MockitoJUnitRunner.class)
public class AffectedEntitiesManagerTest {

    private static final long START_TIME = 1000000L;
    private static final int SHORT_TIMEOUT_MINS = 10;

    @Mock
    private Clock clock;

    @Mock
    private ActionHistoryDao actionHistoryDao;

    @Mock
    private EntitiesInCoolDownPeriodCache entitiesInCoolDownPeriodCache;

    private AffectedEntitiesManager affectedEntitiesManager;

    /**
     * Sets up a AuditedActionsManager with mocks, shared by the tests that can.
     */
    @Before
    public void init() {
        when(clock.millis()).thenReturn(START_TIME);
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(START_TIME));
        when(clock.getZone()).thenReturn(ZoneId.systemDefault());

        affectedEntitiesManager = new AffectedEntitiesManager(
                actionHistoryDao,
                clock,
                SHORT_TIMEOUT_MINS,
                entitiesInCoolDownPeriodCache);
    }

    /**
     * The cache should be initialized using SHORT_TIMEOUT_MINS worth of data.
     */
    @Test
    public void testCacheInitialized() {
        long actionIdCounter = 1L;
        ExecutableStep validExecutableStep = makeExecutableStep(LocalDateTime.now(clock));
        ActionDTO.Action validRecommendation = ActionDTO.Action.newBuilder()
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setResize(ActionDTO.Resize.newBuilder()
                                .buildPartial())
                        .buildPartial())
                .buildPartial();
        ActionView failedAction = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.FAILED,
                validExecutableStep,
                validRecommendation);
        ActionView invalidActionDueToExecutableStep = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.SUCCEEDED,
                null,
                validRecommendation);
        ExecutableStep invalidExecutableStep = makeExecutableStep(null);
        ActionView invalidActionDueToCompletionTime = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.SUCCEEDED,
                invalidExecutableStep,
                validRecommendation);
        ActionView invalidActionDueToRecommendation = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.SUCCEEDED,
                validExecutableStep,
                null);
        ActionDTO.Action invalidReommendationDueToActionInfo = ActionDTO.Action.newBuilder()
                .buildPartial();
        ActionView invalidActionDueToActionInfo = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.SUCCEEDED,
                validExecutableStep,
                invalidReommendationDueToActionInfo);
        ActionView validAction = makeActionView(
                actionIdCounter++,
                ActionDTO.ActionState.SUCCEEDED,
                validExecutableStep,
                validRecommendation);
        ArgumentCaptor<LocalDateTime> startDateTimeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        ArgumentCaptor<LocalDateTime> endDateTimeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        when(actionHistoryDao.getActionHistoryByDate(startDateTimeCaptor.capture(), endDateTimeCaptor.capture()))
                .thenReturn(Arrays.asList(
                    failedAction,
                    invalidActionDueToExecutableStep,
                    invalidActionDueToCompletionTime,
                    invalidActionDueToRecommendation,
                    invalidActionDueToActionInfo,
                    validAction));
        ArgumentCaptor<Long> actionIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<ActionTypeCase> actionTypeCaptor = ArgumentCaptor.forClass(ActionTypeCase.class);
        ArgumentCaptor<EntityActionInfoState> entityActionInfoStateCaptor = ArgumentCaptor.forClass(EntityActionInfoState.class);
        ArgumentCaptor<LocalDateTime> lastUpdatedDatetimeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        doNothing().when(entitiesInCoolDownPeriodCache).insertOrUpdate(
                actionIdCaptor.capture(),
                actionTypeCaptor.capture(),
                entityActionInfoStateCaptor.capture(),
                any(),
                lastUpdatedDatetimeCaptor.capture());

        affectedEntitiesManager = new AffectedEntitiesManager(
                actionHistoryDao,
                clock,
                SHORT_TIMEOUT_MINS,
                entitiesInCoolDownPeriodCache);

        assertEquals(1, startDateTimeCaptor.getAllValues().size());
        assertEquals(LocalDateTime.now(clock).minusMinutes(SHORT_TIMEOUT_MINS), startDateTimeCaptor.getValue());
        assertEquals(LocalDateTime.now(clock), endDateTimeCaptor.getValue());

        assertEquals(1, actionIdCaptor.getAllValues().size());
        assertEquals(validAction.getId(), actionIdCaptor.getValue().longValue());
        assertEquals(validRecommendation.getInfo().getActionTypeCase(), actionTypeCaptor.getValue());
        assertEquals(EntityActionInfoState.SUCCEEDED, entityActionInfoStateCaptor.getValue());
        assertEquals(validExecutableStep.getCompletionTime().get(), lastUpdatedDatetimeCaptor.getValue());
    }

    /**
     * Actions not related to controllable flag should not be placed into the cache.
     */
    @Test
    public void testIrrelevantActionType() {
        ActionView action = makeActionView(
                1L,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                ActionDTO.Action.newBuilder()
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                .setBuyRi(ActionDTO.BuyRI.newBuilder()
                                        .buildPartial())
                                .buildPartial())
                        .buildPartial());
        affectedEntitiesManager.onActionEvent(action,
                null,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                true);
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any());
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any(), any());
    }

    /**
     * Entity types not related to controllable flag should not be placed into the cache.
     */
    @Test
    public void testIrrelevantTargetType() {
        ActionView action = makeActionView(
                1L,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                ActionDTO.Action.newBuilder()
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                .setMove(ActionDTO.Move.newBuilder()
                                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                .setType(CommonDTO.EntityDTO.EntityType.CONTAINER_VALUE)
                                                .buildPartial())
                                        .buildPartial())
                                .buildPartial())
                        .buildPartial());
        affectedEntitiesManager.onActionEvent(action,
                null,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                true);
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any());
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any(), any());
    }

    /**
     * Action states that are not tracked should not be placed into the cache.
     */
    @Test
    public void testIrrelevantActionState() {
        ActionView action = makeActionView(
                1L,
                ActionDTO.ActionState.READY,
                null,
                ActionDTO.Action.newBuilder()
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                .setResize(ActionDTO.Resize.newBuilder()
                                        .buildPartial())
                                .buildPartial())
                        .buildPartial());
        affectedEntitiesManager.onActionEvent(action,
                null,
                ActionDTO.ActionState.READY,
                null,
                true);
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any());
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any(), any());
    }

    /**
     * The affected target entity should be extracted from the resize action.
     */
    @Test
    public void testOneAffectedEntity() {
        ActionView action = makeActionView(
                1L,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                ActionDTO.Action.newBuilder()
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                .setResize(ActionDTO.Resize.newBuilder()
                                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                .setId(2L)
                                                .buildPartial())
                                        .buildPartial())
                                .buildPartial())
                        .buildPartial());
        affectedEntitiesManager.onActionEvent(action,
                null,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                true);
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any());
        verify(entitiesInCoolDownPeriodCache, times(1)).insertOrUpdate(
                eq(1L),
                eq(ActionTypeCase.RESIZE),
                eq(EntityActionInfoState.IN_PROGRESS),
                eq(ImmutableSet.of(2L)),
                eq(null));
    }

    /**
     * The affected target, source, and destination entities should be extracted from the move action.
     */
    @Test
    public void testMultipleAffectedEntities() {
        ActionView action = makeActionView(
                1L,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                ActionDTO.Action.newBuilder()
                        .setInfo(ActionDTO.ActionInfo.newBuilder()
                                .setMove(ActionDTO.Move.newBuilder()
                                        .setTarget(ActionDTO.ActionEntity.newBuilder()
                                                .setId(2L)
                                                .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                                                .buildPartial())
                                        .addChanges(ActionDTO.ChangeProvider.newBuilder()
                                                .setSource(ActionDTO.ActionEntity.newBuilder()
                                                        .setId(3L)
                                                        .buildPartial())
                                                .setDestination(ActionDTO.ActionEntity.newBuilder()
                                                        .setId(4L)
                                                        .buildPartial())
                                                .buildPartial())
                                        .buildPartial())
                                .buildPartial())
                        .buildPartial());
        affectedEntitiesManager.onActionEvent(action,
                null,
                ActionDTO.ActionState.IN_PROGRESS,
                null,
                true);
        verify(entitiesInCoolDownPeriodCache, never()).insertOrUpdate(anyLong(), any(), any(), any());
        verify(entitiesInCoolDownPeriodCache, times(1)).insertOrUpdate(
                eq(1L),
                eq(ActionTypeCase.MOVE),
                eq(EntityActionInfoState.IN_PROGRESS),
                eq(ImmutableSet.of(2L, 3L, 4L)),
                eq(null));
    }

    /**
     * On read request, the manager should cleanup the cache and translate the ActionEffectType to
     * the expected ActionTypeCases.
     */
    @Test
    public void testActionEffectTypeTranslation() {
        affectedEntitiesManager.getAllAffectedEntities(ImmutableMap.of(
            AffectedEntitiesDTO.ActionEffectType.INELIGIBLE_RESIZE_DOWN,
            AffectedEntitiesDTO.AffectedEntitiesTimeoutConfig.newBuilder()
                    .setCompletedActionCoolDownMsec(1L)
                    .setInProgressActionCoolDownMsec(2L)
                    .buildPartial()
        ));
        verify(entitiesInCoolDownPeriodCache, times(1)).removeOldEntries();
        verify(entitiesInCoolDownPeriodCache, times(1)).getAffectedEntitiesByActionType(
                eq(ImmutableSet.of(ActionTypeCase.RESIZE, ActionTypeCase.ATOMICRESIZE)),
                eq(2L),
                eq(1L));
    }

    private ExecutableStep makeExecutableStep(
            @Nullable LocalDateTime completionTime
    ) {
        ExecutableStep executableStep = mock(ExecutableStep.class);
        when(executableStep.getCompletionTime()).thenReturn(Optional.ofNullable(completionTime));
        return executableStep;
    }

    private ActionView makeActionView(
            final long actionId,
            ActionDTO.ActionState actionState,
            @Nullable ExecutableStep executableStep,
            @Nullable ActionDTO.Action recommendation
    ) {
        ActionView mockAction = mock(ActionView.class);
        when(mockAction.getId()).thenReturn(actionId);
        when(mockAction.getState()).thenReturn(actionState);
        when(mockAction.getCurrentExecutableStep()).thenReturn(Optional.ofNullable(executableStep));
        when(mockAction.getRecommendation()).thenReturn(recommendation);
        return mockAction;
    }

}