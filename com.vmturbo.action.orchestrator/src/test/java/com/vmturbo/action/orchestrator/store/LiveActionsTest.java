package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.RejectedActionInfo;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.action.orchestrator.store.LiveActions.QueryFilterFactory;
import com.vmturbo.action.orchestrator.store.query.QueryFilter;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ResourceGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.oid.identity.ArrayOidSet;
import com.vmturbo.oid.identity.OidSet;

public class LiveActionsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    private ActionTargetSelector actionTargetSelector = Mockito.mock(ActionTargetSelector.class);

    private Clock clock = new MutableFixedClock(1_000_000);

    private final QueryFilterFactory queryFilterFactory = mock(QueryFilterFactory.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);
    private final WorkflowStore workflowStore = Mockito.mock(WorkflowStore.class);

    private final InvolvedEntitiesExpander involvedEntitiesExpander = mock(InvolvedEntitiesExpander.class);

    private LiveActions liveActions;

    /**
     * Setup the test instance.
     */
    @Before
    public void setup() {
        liveActions =
                new LiveActions(actionHistoryDao, acceptedActionsStore, rejectedActionsStore, clock,
                        queryFilterFactory, userSessionContext, involvedEntitiesExpander, workflowStore);
        when(involvedEntitiesExpander.expandInvolvedEntitiesFilter(anyCollection())).thenAnswer(
            (Answer<InvolvedEntitiesFilter>)invocationOnMock -> {
                Set<Long> oids = new HashSet<>(
                    (Collection<Long>)(invocationOnMock.getArguments()[0]));
                return new InvolvedEntitiesFilter(
                    oids, InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
            }
        );
    }

    /**
     * Static tests initialization.
     */
    @BeforeClass
    public static void initClass() {
        IdentityGenerator.initPrefix(0);
    }

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
    public void replaceStartSuspendActions() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);
        // Hack - for testing purposes we don't care if it's not actually a start/suspend action.
        liveActions.replaceStartSuspendActions(Stream.of(action1, action2, action3), false);
        assertThat(liveActions.size(), is(3));
        assertThat(liveActions.getAll().collect(Collectors.toList()),
                containsInAnyOrder(action1, action2, action3));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.START_SUSPEND),
                containsInAnyOrder(action1, action2, action3));
    }

    @Test
    public void replaceStartSuspendLiveActions() {
        final Action actionSucceeded = createActionWithCertainState(ActionState.SUCCEEDED, 4);
        final Action actionFailed = createActionWithCertainState(ActionState.FAILED, 5);
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);
        // Hack - for testing purposes we don't care if it's not actually a start/suspend action.
        liveActions.replaceStartSuspendActions(Stream.of(actionSucceeded, actionFailed), false);
        liveActions.replaceStartSuspendActions(Stream.of(action1, action2, action3), true);
        assertThat(liveActions.size(), is(3));
        assertThat(liveActions.getAll().collect(Collectors.toList()),
                containsInAnyOrder(action1, action2, action3));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.START_SUSPEND),
                containsInAnyOrder(action1, action2, action3));
    }

    @Test
    public void testUpdateMarketActions() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);
        final EntitiesAndSettingsSnapshot entityCacheSnapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entityCacheSnapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(entityCacheSnapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());


        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,action1);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,action2);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,action3);

        // Initialize to action1
        liveActions.replaceMarketActions(Stream.of(action1));

        // Remove action1, add action 2 and 3
        liveActions.updateMarketActions(Collections.singleton(action1),
                Arrays.asList(action2, action3), entityCacheSnapshot, actionTargetSelector, Collections.emptyMap());

        assertThat(liveActions.getAll().collect(Collectors.toList()),
            containsInAnyOrder(action2, action3));
    }

    /**
     * Test action state's recovery after restarting action-orchestration component.
     * Accepted actions update their states from READY to ACCEPTED.
     * Actions with removed acceptance update their states from ACCEPTED to READY.
     *
     * @throws UnsupportedActionException if something goes wrong
     */
    @Test
    public void testUpdatingActionStates() throws UnsupportedActionException {
        final long scheduleId = 505L;
        final ScheduleProto.Schedule actionSchedule1 = createSchedule(scheduleId);

        final Action acceptedAction = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        acceptedAction.getActionTranslation().setPassthroughTranslationSuccess();
        final Action actionWithRemovedAcceptance =
            createActionWithCertainState(ActionState.ACCEPTED, 2);
        final Action acceptedActionWithRemovedSchedule =
            createActionWithCertainState(ActionState.ACCEPTED, 3);

        final EntitiesAndSettingsSnapshot entityCacheSnapshot =
                mock(EntitiesAndSettingsSnapshot.class);
        Mockito.when(entityCacheSnapshot.getOwnerAccountOfEntity(anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(anyLong()))
                .thenReturn(Optional.empty());

        final Map<String, Setting> settingMap =
                ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                        ActionMode.MANUAL, Collections.singleton(scheduleId));
        final Map<String, Setting> settingMapNoSchedule =
            ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                ActionMode.MANUAL, Collections.emptyList());
        final ActionEntity acceptedActionEntity =
                ActionDTOUtil.getPrimaryEntity(acceptedAction.getRecommendation());
        final ActionEntity actionWithRemovedAcceptanceEntity =
                ActionDTOUtil.getPrimaryEntity(actionWithRemovedAcceptance.getRecommendation());
        final ActionEntity acceptedActionWithRemovedScheduleEntity =
            ActionDTOUtil.getPrimaryEntity(acceptedActionWithRemovedSchedule.getRecommendation());
        Mockito.when(entityCacheSnapshot.getSettingsForEntity(acceptedActionEntity.getId()))
                .thenReturn(settingMap);
        Mockito.when(
                entityCacheSnapshot.getSettingsForEntity(actionWithRemovedAcceptanceEntity.getId()))
                .thenReturn(settingMap);
        Mockito.when(
            entityCacheSnapshot.getSettingsForEntity(acceptedActionWithRemovedScheduleEntity.getId()))
            .thenReturn(settingMapNoSchedule);

        Mockito.when(entityCacheSnapshot.getScheduleMap())
                .thenReturn(ImmutableMap.of(scheduleId, actionSchedule1));
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(
                acceptedAction.getRecommendationOid())).thenReturn(Optional.of("admin"));
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(
                actionWithRemovedAcceptance.getRecommendationOid())).thenReturn(Optional.empty());
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(
            acceptedActionWithRemovedSchedule.getRecommendationOid())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                acceptedAction);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                actionWithRemovedAcceptance);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
            acceptedActionWithRemovedSchedule);

        Mockito.when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
                .thenAnswer(invocation -> {
                    Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
                    return actions.collect(Collectors.toMap(ActionDTO.Action::getId,
                            action -> ImmutableActionTargetInfo.builder()
                                    .targetId(100L)
                                    .supportingLevel(SupportLevel.SUPPORTED)
                                    .build()));
                });

        Assert.assertEquals(ActionState.READY, acceptedAction.getState());
        Assert.assertEquals(ActionState.ACCEPTED, actionWithRemovedAcceptance.getState());
        Assert.assertEquals(ActionState.ACCEPTED, acceptedActionWithRemovedSchedule.getState());

        // ACT
        liveActions.updateMarketActions(Collections.emptyList(),
                Arrays.asList(acceptedAction, actionWithRemovedAcceptance, acceptedActionWithRemovedSchedule),
                entityCacheSnapshot,
                actionTargetSelector, Collections.emptyMap());

        Assert.assertThat(liveActions.getAll().collect(Collectors.toList()),
                containsInAnyOrder(acceptedAction, actionWithRemovedAcceptance, acceptedActionWithRemovedSchedule));
        Assert.assertEquals(ActionState.ACCEPTED, acceptedAction.getState());
        Assert.assertEquals(ActionState.READY, actionWithRemovedAcceptance.getState());
        Assert.assertEquals(ActionState.READY, acceptedActionWithRemovedSchedule.getState());
    }

    /**
     * Test action state's recovery after restarting action-orchestration component.
     * Rejected actions update their states from READY to REJECTED.
     * Actions with removed rejection update their states from REJECTED to READY.
     */
    @Test
    public void testUpdatingActionStatesForRejectedActions() {
        final Action rejectedAction = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        rejectedAction.getActionTranslation().setPassthroughTranslationSuccess();
        final Action actionWithRemovedRejection =
                createActionWithCertainState(ActionState.REJECTED, 2);

        final EntitiesAndSettingsSnapshot entityCacheSnapshot =
                mock(EntitiesAndSettingsSnapshot.class);
        Mockito.when(entityCacheSnapshot.getOwnerAccountOfEntity(anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(anyLong()))
                .thenReturn(Optional.empty());

        Mockito.when(rejectedActionsStore.getAllRejectedActions())
                .thenReturn(Collections.singletonList(
                        new RejectedActionInfo(rejectedAction.getRecommendationOid(), "Rejector",
                                LocalDateTime.now(), StringConstants.TURBO_USER_TYPE,
                                Collections.singleton(1121L))));

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                rejectedAction);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                actionWithRemovedRejection);

        Assert.assertEquals(ActionState.READY, rejectedAction.getState());
        Assert.assertEquals(ActionState.REJECTED, actionWithRemovedRejection.getState());

        // ACT
        liveActions.updateMarketActions(Collections.emptyList(),
                Arrays.asList(rejectedAction, actionWithRemovedRejection), entityCacheSnapshot,
                actionTargetSelector, Collections.emptyMap());

        Assert.assertThat(liveActions.getAll().collect(Collectors.toList()),
                containsInAnyOrder(rejectedAction, actionWithRemovedRejection));
        Assert.assertEquals(ActionState.REJECTED, rejectedAction.getState());
        Assert.assertEquals(ActionState.READY, actionWithRemovedRejection.getState());
    }

    /**
     * Tests updating latest recommendation time for accepted actions.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdateLatestRecommendationTimeForAcceptedActions() throws Exception {
        final long scheduleId = 505L;
        final ScheduleProto.Schedule actionSchedule1 = createSchedule(scheduleId);

        final Action acceptedAction = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        acceptedAction.getActionTranslation().setPassthroughTranslationSuccess();
        final Action actionWithoutAcceptance = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final EntitiesAndSettingsSnapshot entityCacheSnapshot =
                mock(EntitiesAndSettingsSnapshot.class);
        when(entityCacheSnapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                acceptedAction);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entityCacheSnapshot,
                actionWithoutAcceptance);

        Mockito.when(entityCacheSnapshot.getScheduleMap())
                .thenReturn(ImmutableMap.of(scheduleId, actionSchedule1));
        Mockito.when(entityCacheSnapshot.getAcceptingUserForAction(
                acceptedAction.getRecommendationOid())).thenReturn(Optional.of("admin"));

        final Map<String, Setting> settingMap =
                ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                        ActionMode.MANUAL, Collections.singleton(scheduleId));
        final ActionEntity acceptedActionEntity =
                ActionDTOUtil.getPrimaryEntity(acceptedAction.getRecommendation());
        Mockito.when(entityCacheSnapshot.getSettingsForEntity(acceptedActionEntity.getId()))
                .thenReturn(settingMap);

        Mockito.when(actionTargetSelector.getTargetsForActions(any(), any(), any()))
                .thenAnswer(invocation -> {
                    Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
                    return actions.collect(Collectors.toMap(ActionDTO.Action::getId,
                            action -> ImmutableActionTargetInfo.builder()
                                    .targetId(100L)
                                    .supportingLevel(SupportLevel.SUPPORTED)
                                    .build()));
                });

        liveActions.updateMarketActions(Collections.emptyList(),
                Arrays.asList(acceptedAction, actionWithoutAcceptance), entityCacheSnapshot,
                actionTargetSelector, Collections.emptyMap());

        assertThat(liveActions.getAll().collect(Collectors.toList()),
                containsInAnyOrder(acceptedAction, actionWithoutAcceptance));
        Mockito.verify(acceptedActionsStore)
                .updateLatestRecommendationTime(
                        Collections.singletonList(acceptedAction.getRecommendationOid()));
    }

    @Test
    public void testSize() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));
        liveActions.replaceStartSuspendActions(Stream.of(action3),false);
        assertThat(liveActions.size(), is(3));
    }

    @Test
    public void testGet() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));
        liveActions.replaceStartSuspendActions(Stream.of(action3),false);

        assertThat(liveActions.get(action1.getId()).get(), is(action1));
        assertThat(liveActions.get(action2.getId()).get(), is(action2));
        assertThat(liveActions.get(action3.getId()).get(), is(action3));
        assertFalse(liveActions.get(action1.getId() + 100).isPresent());
    }

    @Test
    public void testGetWithScopedUser() {
        final Action move12Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 1, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 0, 1, 0, 3, 0),
                1);
        final Action move34Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(3, 6, 3, 0, 4, 0),
                1);
        final Action move45Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(4, 0, 3, 0, 5, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move12Action, move34Action));
        liveActions.replaceRiActions(Stream.of(move13Action));
        liveActions.replaceStartSuspendActions(Stream.of(move45Action), false);


        // verify that a user with access to entities 0,1 and 2 can fetch move12 (in scope), move13 (partially in scope)
        // but not move34 (out of scope)
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L)), null));
        assertThat(liveActions.get(move12Action.getId()).get(), is(move12Action));
        assertThat(liveActions.get(move13Action.getId()).get(), is(move13Action));
        assertThat(liveActions.get(move45Action.getId()).get(), is(move45Action));

        // a request for move34 by this user will trigger an access exception
        expectedException.expect(UserAccessScopeException.class);
        assertThat(liveActions.get(move34Action.getId()).get(), is(move34Action));
    }

    @Test
    public void testMultiGet() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);
        final Action action4 = ActionOrchestratorTestUtils.createMoveAction(4, 2);

        liveActions.replaceMarketActions(Stream.of(action1, action2));
        liveActions.replaceRiActions(Stream.of(action3));
        liveActions.replaceStartSuspendActions(Stream.of(action4), false);

        // Try to get one from market, one from RI and one from start/suspend.
        // Shouldn't return the other one from market.
        assertThat(
                liveActions.get(Sets.newHashSet(action1.getId(), action3.getId(), action4.getId()))
                        .collect(Collectors.toList()),
                containsInAnyOrder(action1, action3, action4));
    }

    @Test
    public void testMultiGetWithScopedUser() {
        final Action move12Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 1, 0, 2, 0),
                1);
        final Action move14Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 0, 1, 0, 4, 0),
                1);
        final Action move15Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(3, 0, 1, 0, 5, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move12Action));
        liveActions.replaceRiActions(Stream.of(move14Action));
        liveActions.replaceStartSuspendActions(Stream.of(move15Action), false);

        // verify that a user with access to entities 0,1,2 and 3 can see both actions
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L, 3L)), null));

        assertThat(liveActions.get(
                                Sets.newHashSet(move12Action.getId(), move14Action.getId(), move15Action.getId()))
                        .collect(Collectors.toList()),
                containsInAnyOrder(move12Action, move14Action, move15Action));

        // a user w/access to only 2 and 3 will get an exception when trying to access all the actions
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(2L, 3L)), null));
        expectedException.expect(UserAccessScopeException.class);
        liveActions.get(Sets.newHashSet(move12Action.getId(), move14Action.getId(), move15Action.getId()));
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

        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

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

        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

        // We should only get action1 - action 2 shouldn't even be considered by the query filter.
        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()), containsInAnyOrder(action1));
        verify(queryFilter, never()).test(action2);
    }

    @Test
    public void testGetByFilterWithStartEndDateNoInvolvedEntities() {
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

        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setStartDate(start.toEpochMilli())
                .setEndDate(end.toEpochMilli())
                .build();
        when(actionHistoryDao.getActionHistoryByFilter(actionQueryFilter))
            .thenReturn(Arrays.asList(historicalAction, badHistoricalAction));

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(liveAction)).thenReturn(true);
        when(queryFilter.test(historicalAction)).thenReturn(true);
        when(queryFilter.test(badLiveAction)).thenReturn(false);
        when(queryFilter.test(badHistoricalAction)).thenReturn(false);
        // The succeeded, failed, and late actions shouldn't reach the query filter.
        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()),
            containsInAnyOrder(liveAction, historicalAction));
        verify(queryFilter, never()).test(succeededAction);
        verify(queryFilter, never()).test(failedAction);
        verify(queryFilter, never()).test(lateRecommendedAction);
    }

    @Test
    public void testGetByFilterInvolvedEntitiesAndScopeFilter() throws UnsupportedActionException {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(100, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(200, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        final QueryFilter queryFilter = mock(QueryFilter.class);
        // Suppose both actions would have passed the query filter.
        when(queryFilter.test(action1)).thenReturn(true);
        when(queryFilter.test(action2)).thenReturn(true);

        final Instant start = clock.instant().minusMillis(100);
        final Instant end = clock.instant().plusMillis(100);
        final LocalDateTime startDate = LocalDateTime.ofInstant(start, clock.getZone());
        final LocalDateTime endDate = LocalDateTime.ofInstant(end, clock.getZone());
        // Target only action 1's entity.
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setInvolvedEntities(InvolvedEntities.newBuilder()
                        .addOids(ActionDTOUtil.getPrimaryEntity(action1.getRecommendation()).getId()))
                .setResourceGroupFilter(ResourceGroupFilter.newBuilder().addResourceGroupOid(300))
                .setStartDate(start.toEpochMilli())
                .setEndDate(end.toEpochMilli())
                .build();

        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE),
                any(), any())).thenReturn(queryFilter);

        // When an organizational scope filter (such as AccountFilter, ResourceGrpupFilter,..) along with dates is set, it has
        // precedence and historical actions pertaining to that scope will be returned.
        when(actionHistoryDao.getActionHistoryByFilter(actionQueryFilter)).thenReturn(Arrays.asList(action1, action2));

        assertThat(liveActions.get(actionQueryFilter).collect(Collectors.toList()), containsInAnyOrder(action1, action2));
        verify(queryFilter, times(1)).test(action1);
        verify(queryFilter, times(1)).test(action2);

        // Repeat test without start end dates.  We should only get action1 - action 2 shouldn't even be considered
        // by the query filter, as the OID of the target that action is not in the involved entities set, even if it's
        // in the historical actions for the Resource Group specified.  Hence for Live Actions, the Involved Entities
        // determine what's fetched.
        final ActionQueryFilter actionQueryFilterWithoutDates = ActionQueryFilter.newBuilder()
                .setInvolvedEntities(InvolvedEntities.newBuilder()
                        .addOids(ActionDTOUtil.getPrimaryEntity(action1.getRecommendation()).getId()))
                .setResourceGroupFilter(ResourceGroupFilter.newBuilder().addResourceGroupOid(300))
                .build();
        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilterWithoutDates), eq(LiveActionStore.VISIBILITY_PREDICATE),
                any(), any())).thenReturn(queryFilter);
        when(actionHistoryDao.getActionHistoryByFilter(actionQueryFilterWithoutDates))
                .thenReturn(Arrays.asList(action1, action2));

        assertThat(liveActions.get(actionQueryFilterWithoutDates).collect(Collectors.toList()), containsInAnyOrder(action1));
        // Only action1 is valid in this case.
        verify(queryFilter, times(2)).test(action1);
        verify(queryFilter, times(1)).test(action2);
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

        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setStartDate(start.toEpochMilli())
                .setEndDate(end.toEpochMilli())
                .setInvolvedEntities(InvolvedEntities.newBuilder()
                        .addAllOids(ActionDTOUtil.getInvolvedEntityIds(liveAction.getRecommendation()))
                        .addAllOids(ActionDTOUtil.getInvolvedEntityIds(historicalAction.getRecommendation())))
                .build();
        when(actionHistoryDao.getActionHistoryByFilter(actionQueryFilter))
            .thenReturn(Arrays.asList(historicalAction, badHistoricalAction));

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(liveAction)).thenReturn(true);
        when(queryFilter.test(historicalAction)).thenReturn(true);
        when(queryFilter.test(badHistoricalAction)).thenReturn(false);
        // The entity with no desired involved entities shouldn't be considered by the filter.

        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

        List<ActionView> results = liveActions.get(actionQueryFilter).collect(Collectors.toList());

        assertThat(results, containsInAnyOrder(liveAction, historicalAction));
        verify(queryFilter, never()).test(noInvolvedEntitiesLiveAction);
    }

    /**
     * Should consider expanded involved entities in target and source of action.
     */
    @Test
    public void testExpandedInvolvedEntities() {
        final Action moveActionInTarget = ActionOrchestratorTestUtils.actionFromRecommendation(
            ActionOrchestratorTestUtils.createMoveRecommendation(1, 11, 21, 0, 31, 0),
            1);
        final Action moveActionInSource = ActionOrchestratorTestUtils.actionFromRecommendation(
            ActionOrchestratorTestUtils.createMoveRecommendation(2, 12, 22, 0, 32, 0),
            1);
        final Action moveActionInDestination = ActionOrchestratorTestUtils.actionFromRecommendation(
            ActionOrchestratorTestUtils.createMoveRecommendation(3, 13, 23, 0, 33, 0),
            1);

        liveActions =
                new LiveActions(actionHistoryDao, acceptedActionsStore, rejectedActionsStore, clock,
                        QueryFilter::new, userSessionContext, involvedEntitiesExpander, workflowStore);
        liveActions.replaceMarketActions(Stream.of(
            moveActionInTarget, moveActionInSource, moveActionInDestination));

        when(involvedEntitiesExpander.expandInvolvedEntitiesFilter(anyCollection())).thenReturn(
            new InvolvedEntitiesFilter(ImmutableSet.of(11L, 22L, 33L),
                InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS)
        );

        List<Long> actualActions = liveActions.get(ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addOids(1L)
                .build())
            .buildPartial())
            .map(ActionView::getId)
            .collect(Collectors.toList());

        // Action oid 3 should not be included because the expanded involved entity participates
        // in the destination.
        Assert.assertEquals(2, actualActions.size());
        Assert.assertEquals(ImmutableSet.of(1L, 2L), ImmutableSet.copyOf(actualActions));
    }

    @Test
    public void testGetByFilterNoDatesWithScopedUser() {
        final Action move02Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 0, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 1, 1, 0, 3, 0),
                1);
        final Action move04Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(3, 0, 0, 0, 4, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move02Action));
        liveActions.replaceRiActions(Stream.of(move13Action));
        liveActions.replaceStartSuspendActions(Stream.of(move04Action), false);

        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(any())).thenReturn(true);
        // Target all entities.
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();
        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

        // verify that a user with access to entities 0,1 and 2 can see both actions.
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L)), null));
        Set<ActionView> results = liveActions.get(actionQueryFilter).collect(Collectors.toSet());
        assertTrue(results.contains(move02Action));
        assertTrue(results.contains(move13Action));
        assertTrue(results.contains(move04Action));

        // a user w/access to only 0 and 2 will only get move02 but not move13
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 2L)), null));
        results = liveActions.get(actionQueryFilter).collect(Collectors.toSet());
        assertTrue(results.contains(move02Action));
        assertTrue(results.contains(move04Action));
        assertFalse(results.contains(move13Action));
    }

    @Test
    public void testGetByEntity() throws UnsupportedActionException {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(100, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(200, 2);
        liveActions.replaceMarketActions(Stream.of(action1, action2));

        ActionDTOUtil.getInvolvedEntityIds(action1.getRecommendation())
            .forEach(involvedEntityId -> {
                assertThat(liveActions.getByEntity(Lists.newArrayList(involvedEntityId)).findFirst().get(), is(action1));
            });
        ActionDTOUtil.getInvolvedEntityIds(action2.getRecommendation())
            .forEach(involvedEntityId -> {
                assertThat(liveActions.getByEntity(Lists.newArrayList(involvedEntityId)).findFirst().get(), is(action2));
            });

        assertThat(liveActions.getByEntity(Lists.newArrayList(
            ActionDTOUtil.getPrimaryEntityId(action1.getRecommendation()),
            ActionDTOUtil.getPrimaryEntityId(action2.getRecommendation())))
                .collect(Collectors.toList()), containsInAnyOrder(action1, action2));

    }

    @Test
    public void testGetByEntityWithScopedUser() {
        final Action move12Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 1, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 2, 1, 0, 3, 0),
                1);
        final Action move34Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(3, 6, 3, 0, 4, 0),
                1);
        final Action move15Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(4, 2, 1, 0, 5, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move12Action, move34Action));
        liveActions.replaceRiActions(Stream.of(move13Action));
        liveActions.replaceStartSuspendActions(Stream.of(move15Action), false);

        // verify that a user with access to entities 0, 1, 2 and 3 can see all three actions
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L, 3L)), null));

        assertThat(liveActions.getByEntity(Lists.newArrayList(0L, 1L, 3L)).collect(Collectors.toSet()),
                containsInAnyOrder(move12Action, move13Action, move34Action, move15Action));

        // a user w/access to only entities 0, 1, 6 will only get actions related to 0, 1 if they
        // try to access actions for entities 0, 1, 2, where entity 2 is not accessible.
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 6L)), null));
        Set<ActionView> results = liveActions.getByEntity(Lists.newArrayList(0L, 1L, 2L)).collect(Collectors.toSet());
        assertThat(results, containsInAnyOrder(move12Action, move13Action, move15Action));
        assertFalse(results.contains(move34Action));

        // the same user would be able to see the move13 action when they request actions for entity
        // 1, even though entity 3 is outside their normal scope.
        results = liveActions.getByEntity(Lists.newArrayList(0L, 1L)).collect(Collectors.toSet());
        assertThat(results, containsInAnyOrder(move12Action, move13Action, move15Action));
        assertFalse(results.contains(move34Action));
    }

    @Test
    public void isEmpty() {
        assertTrue(liveActions.isEmpty());
    }

    /**
     * Test that {@link LiveActions#getAction(long)} and
     * {@link LiveActions#getActionByRecommendationId(long)} return the same action
     * regardless searching by actionId or by recommendationId.
     */
    @Test
    public void testGetActionAndGetActionByRecommendationId() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));
        liveActions.replaceStartSuspendActions(Stream.of(action3), false);

        assertThat(liveActions.getAction(action1.getId()).get(), is(action1));
        assertThat(liveActions.getAction(action2.getId()).get(), is(action2));
        assertThat(liveActions.getAction(action3.getId()).get(), is(action3));
        assertThat(liveActions.getActionByRecommendationId(action1.getRecommendationOid()).get(),
                is(action1));
        assertThat(liveActions.getActionByRecommendationId(action2.getRecommendationOid()).get(),
                is(action2));
        assertThat(liveActions.getActionByRecommendationId(action3.getRecommendationOid()).get(),
                is(action3));
        assertFalse(liveActions.getAction(action1.getId() + 100).isPresent());
    }

    /**
     * Test that {@link LiveActions#get(ActionQueryFilter)}
     */
    @Test
    public void testGetByFilterWithExpandedInvolvedEntitiesWithScopedUser() {
        final Action move02Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 0, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 1, 1, 0, 3, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move02Action, move13Action));

        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(1L, 3L, 2L)), null));
        final QueryFilter queryFilter = mock(QueryFilter.class);
        when(queryFilter.test(any())).thenReturn(true);
        final ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setInvolvedEntities(InvolvedEntities.newBuilder()
                        .addOids(0L).addOids(1L).addOids(3L).build())
                .build();
        when(queryFilterFactory.newQueryFilter(eq(actionQueryFilter), eq(LiveActionStore.VISIBILITY_PREDICATE), any(), any())).thenReturn(queryFilter);

        Set<ActionView> results = liveActions.get(actionQueryFilter).collect(Collectors.toSet());
        assertTrue(results.contains(move13Action));
        assertFalse(results.contains(move02Action));
    }

    @Test
    public void getActionWithScopedUser() {
        final Action move12Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 1, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 0, 1, 0, 3, 0),
                1);
        final Action move34Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(3, 6, 3, 0, 4, 0),
                1);

        liveActions.replaceMarketActions(Stream.of(move12Action, move13Action, move34Action));

        // verify that a user with access to entities 0,1 and 2 can fetch move12 and move13 but not move34
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L)), null));
        assertThat(liveActions.getAction(move12Action.getId()).get(), is(move12Action));
        // scoped user can access move 13, even though only one involved entity is in scope
        assertThat(liveActions.getAction(move13Action.getId()).get(), is(move13Action));

        // the request for move34 will trigger an access exception
        expectedException.expect(UserAccessScopeException.class);
        assertThat(liveActions.getAction(move34Action.getId()).get(), is(move34Action));
    }

    @Test
    public void getActionsByPlanType() {
        final Action action1 = ActionOrchestratorTestUtils.createMoveAction(1, 2);
        final Action action2 = ActionOrchestratorTestUtils.createMoveAction(2, 2);
        final Action action3 = ActionOrchestratorTestUtils.createMoveAction(3, 2);

        liveActions.replaceMarketActions(Stream.of(action1));
        liveActions.replaceRiActions(Stream.of(action2));
        liveActions.replaceStartSuspendActions(Stream.of(action3), false);

        assertThat(liveActions.getActionsByPlanType().keySet(),
                containsInAnyOrder(ActionPlanType.MARKET, ActionPlanType.BUY_RI, ActionPlanType.START_SUSPEND ));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.MARKET), containsInAnyOrder(action1));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.BUY_RI), containsInAnyOrder(action2));
        assertThat(liveActions.getActionsByPlanType().get(ActionPlanType.START_SUSPEND),
                containsInAnyOrder(action3));
    }

    @Test
    public void getActionsInvolvingCloudStaticOids() {
        final Action move12Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 0, 1, 0, 2, 0),
                1);
        final Action move13Action = ActionOrchestratorTestUtils.actionFromRecommendation(
                ActionOrchestratorTestUtils.createMoveRecommendation(2, 0, 1,
                    ApiEntityType.COMPUTE_TIER.typeNumber(), 3,
                    ApiEntityType.COMPUTE_TIER.typeNumber()),
                1);
        liveActions.replaceMarketActions(Stream.of(move12Action, move13Action));

        Map<Integer, OidSet> cloudStaticOidSet = ImmutableMap.of(ApiEntityType.COMPUTE_TIER.typeNumber(),
                new ArrayOidSet(Arrays.asList(1L, 3L)));
        // verify that a user with access to entities 0,1 and 2 can fetch move12  but not move13
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L)), cloudStaticOidSet ));

        assertThat(liveActions.getAction(move12Action.getId()).get(), is(move12Action));
        // scoped user can access move 13, even though only one involved entity is in scope
        assertThat(liveActions.getAction(move13Action.getId()).get(), is(move13Action));
    }

    private Action createActionWithCertainState(@Nonnull ActionState actionState, long actionOid) {
        final ActionDTO.Action recommendation =
                ActionOrchestratorTestUtils.createMoveRecommendation(actionOid);

        final SerializationState actionSerializedState =
                new SerializationState(2L, recommendation, LocalDateTime.now(),
                        ActionDecision.getDefaultInstance(), null, actionState,
                        new ActionTranslation(recommendation), null, null,
                        "Move VM action".getBytes(), 2244L);
        return ActionOrchestratorTestUtils.createActionFromSerializedState(actionSerializedState);
    }

    private Schedule createSchedule(long scheduleId) {
        return ScheduleProto.Schedule.newBuilder()
                .setId(scheduleId)
                .setDisplayName("TestSchedule")
                .setNextOccurrence(ScheduleProto.Schedule.NextOccurrence.newBuilder()
                        .setStartTime(System.currentTimeMillis())
                        .build())
                .setActive(ScheduleProto.Schedule.Active.newBuilder()
                        .setRemainingActiveTimeMs(150000L))
                .setStartTime(1588441640000L)
                .setEndTime(1588445240000L)
                .setTimezoneId("America/Toronto")
                .build();
    }
}
