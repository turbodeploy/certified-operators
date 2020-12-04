package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.LiveActions.RecommendationTracker;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModel;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator;
import com.vmturbo.action.orchestrator.store.identity.IdentityDataStore;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.identity.InMemoryIdentityStore;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec.CommodityMergeData;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Integration tests related to the LiveActionStore.
 */
public class LiveActionStoreTest {

    private final long topologyId = 0xDEADEEF;
    private final long firstPlanId = 0xBEADED;
    private final long secondPlanId = 0xDADDA;

    private final long vm1 = 1;
    private final long vm2 = 2;
    private final long vm3 = 3;
    private final long vm4 = 4;

    private final long container1 = 11;
    private final long container2 = 12;
    private final long container3 = 13;
    private final long container4 = 14;
    private final long controller1 = 31;
    private final long containerSpec1 = 41;
    private final long containerSpec2 = 42;
    private final long pod1 = 51;

    private final long hostA = 0xA;
    private final long hostB = 0xB;
    private final long hostC = 0xC;
    private final long hostD = 0xD;
    private final int vmType = 1;

    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private IdentityServiceImpl<ActionInfo, String, ActionInfoModel> actionIdentityService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Permit spying on actions inserted into the store so that their state can be mocked
     * out for testing purposes.
     */
    private class SpyActionFactory implements IActionFactory {
        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Action newAction(@Nonnull final ActionDTO.Action recommendation, long actionPlanId,
                long recommendationOid) {
            return spy(new Action(recommendation, actionPlanId, actionModeCalculator,
                    recommendationOid));
        }

        @Nonnull
        @Override
        public Action newPlanAction(@Nonnull ActionDTO.Action recommendation, @Nonnull LocalDateTime recommendationTime,
                                    long actionPlanId, String description,
                                    @Nullable final Long associatedAccountId, @Nullable final Long associatedResourceGroupId) {
            return spy(new Action(recommendation, recommendationTime, actionPlanId,
                    actionModeCalculator, description, associatedAccountId,
                    associatedResourceGroupId, IdentityGenerator.next()));
        }
    }

    private static final long TOPOLOGY_CONTEXT_ID = 123456;

    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();

    private final ActionTargetSelector targetSelector = Mockito.mock(ActionTargetSelector.class);

    private final ProbeCapabilityCache probeCapabilityCache = Mockito.mock(ProbeCapabilityCache.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private LiveActionStore actionStore;

    private LiveActionsStatistician actionsStatistician = mock(LiveActionsStatistician.class);

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    private Clock clock = new MutableFixedClock(1_000_000);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());

    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private final InvolvedEntitiesExpander involvedEntitiesExpander =
        mock(InvolvedEntitiesExpander.class);

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        supplyChainServiceMole,
        repositoryServiceMole);

    private final AcceptedActionsDAO acceptedActionsStore = mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = mock(RejectedActionsDAO.class);

    private ActionTopologyStore actionTopologyStore = new ActionTopologyStore();
    private EntitySeverityCache entitySeverityCache = mock(EntitySeverityCache.class);
    private WorkflowStore workflowStore = mock(WorkflowStore.class);

    final AtomicActionSpecsCache atomicActionSpecsCache = Mockito.spy(new AtomicActionSpecsCache());
    final AtomicActionFactory atomicActionFactory = Mockito.spy(new AtomicActionFactory(atomicActionSpecsCache));
    private ActionEntity aggregateEntity1;
    private ActionEntity deDupEntity1;
    private ActionEntity deDupEntity2;
    private Collection<Long> atomicActionTargetEntities;
    private Collection<Long> actionPlanTargetEntities;
    private ActionPlanInfo actionPlanInfo;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        // license check client will default to acting as if a valid license is installed.
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(true);
        final IdentityDataStore<ActionInfoModel> idDataStore = new InMemoryIdentityStore<>();
        this.actionIdentityService =
                new IdentityServiceImpl<>(idDataStore, new ActionInfoModelCreator(),
                   ActionInfoModel::getActionHexHash, Clock.systemUTC(), 1000);
        actionStore = new LiveActionStore(spyActionFactory, TOPOLOGY_CONTEXT_ID,
                targetSelector,
                probeCapabilityCache, entitySettingsCache, actionHistoryDao, actionsStatistician,
                actionTranslator, atomicActionFactory, clock, userSessionContext,
                licenseCheckClient, acceptedActionsStore, rejectedActionsStore,
                actionIdentityService, involvedEntitiesExpander,
                Mockito.mock(ActionAuditSender.class), entitySeverityCache, 60, workflowStore);

        when(targetSelector.getTargetsForActions(any(), any(), any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions
                .collect(Collectors.toMap(ActionDTO.Action::getId, action -> ImmutableActionTargetInfo.builder()
                    .supportingLevel(SupportLevel.SUPPORTED)
                    .build()));
        });
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());
        setEntitiesOIDs();
        IdentityGenerator.initPrefix(0);
        setUpActionMergeCache();
    }

    private static ActionDTO.Action.Builder move(long targetId,
                                                 long sourceId, int sourceType,
                                                 long destinationId, int destinationType) {
        return ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),
            targetId, sourceId, sourceType, destinationId, destinationType).toBuilder();
    }

    private static ActionDTO.Action.Builder provision(long targetId,
                                                      int entityType) {
        return ActionOrchestratorTestUtils.createProvisionRecommendation(IdentityGenerator.next(),
                targetId, entityType).toBuilder();
    }

    public void setEntitiesOIDs() {
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getEntityFromOid(eq(vm1)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm1,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm2)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm2,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm3)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm3,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm4)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm4,
                        EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostA)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostA,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostB)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostB,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostC)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostC,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostD)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostD,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(container1)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(container1,
                        EntityType.CONTAINER.getNumber()));
        when(snapshot.getEntityFromOid(eq(container2)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(container2,
                        EntityType.CONTAINER.getNumber()));
        when(snapshot.getEntityFromOid(eq(controller1)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(controller1,
                        EntityType.WORKLOAD_CONTROLLER.getNumber()));
        when(snapshot.getEntityFromOid(eq(containerSpec1)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(containerSpec1,
                        EntityType.CONTAINER_SPEC.getNumber()));
        when(snapshot.getEntityFromOid(eq(containerSpec2)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(containerSpec2,
                        EntityType.CONTAINER_SPEC.getNumber()));
        when(snapshot.getEntityFromOid(eq(container3)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(container3,
                        EntityType.CONTAINER.getNumber()));
        when(snapshot.getEntityFromOid(eq(container4)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(container4,
                        EntityType.CONTAINER.getNumber()));
        when(snapshot.getEntityFromOid(eq(pod1)))
                .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(pod1,
                        EntityType.CONTAINER_POD.getNumber()));
    }

    /**
     * Verify that execution is allowed when the license is valid.
     */
    @Test
    public void testValidLicense() {
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(true);
        assertTrue(actionStore.allowsExecution());
    }


    /**
     * Verify that execution is disallowed when the license is invalid.
     */
    @Test
    public void testInvalidLicense() {
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(false);
        assertFalse(actionStore.allowsExecution());
    }

    @Test
    public void testPopulateRecommendedActionsFromEmpty() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm2, hostB, vmType, hostC, vmType))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
        final Action action = actionStore.getActions().values().iterator().next();
        final Optional<Action> actionOpt =
                actionStore.getActionByRecommendationId(action.getRecommendationOid());
        Assert.assertTrue(actionOpt.isPresent());
        Assert.assertEquals(action, actionOpt.get());
    }

    @Test
    public void testPopulateWithRepeatsAddsDuplicates() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        assertEquals(2, actionStore.size());
    }

    @Test
    public void testPopulatePreservesReRecommended() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(1, actionStore.size());
        assertTrue(actionStore.getAction(firstMove.getId()).isPresent());
    }

    @Test
    public void testPopulateNotRecommendedAreClearedAndRemoved() throws Exception {
        // Can't use spies when checking for action state because action state machine will call
        // methods in the original action, not in the spy.
        final ActionStore actionStore =
                new LiveActionStore(new ActionFactory(actionModeCalculator), TOPOLOGY_CONTEXT_ID,
                        targetSelector, probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                        actionsStatistician, actionTranslator, atomicActionFactory, clock,
                        userSessionContext, licenseCheckClient, acceptedActionsStore,
                        rejectedActionsStore, actionIdentityService, involvedEntitiesExpander,
                        Mockito.mock(ActionAuditSender.class), entitySeverityCache, 60, workflowStore);

        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        Action actionToClear = actionStore.getAction(firstMove.getId()).get();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
        assertEquals(ActionState.CLEARED, actionToClear.getState());
    }

    /**
     * The goal of this test is to verify that actions that are generated with a READY state make it to the audit
     * on generation step. If they don't have a workflow, they won't be sent to SNOW. However, before the bug prevented
     * actions from being sent to SNOW since they were not being sent to even the `sendActionEvents()` method.
     *
     * @throws Exception If the action is not populated or sent to SNOW Audit.
     */
    @Test
    public void testAuditActionSendReady() throws Exception {
        final ActionAuditSender listener = Mockito.mock(ActionAuditSender.class);

        final ActionStore actionStore =
                new LiveActionStore(new ActionFactory(actionModeCalculator), TOPOLOGY_CONTEXT_ID,
                        targetSelector, probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                        actionsStatistician, actionTranslator, atomicActionFactory, clock,
                        userSessionContext, licenseCheckClient, acceptedActionsStore,
                        rejectedActionsStore, actionIdentityService, involvedEntitiesExpander,
                        listener, entitySeverityCache, 60, workflowStore);

        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                                        .setTopologyId(topologyId))))
                .setId(firstPlanId)
                .addAction(firstMove)
                .build();

        final EntitiesAndSettingsSnapshot snapshot =
                entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                        Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        Mockito.doNothing().when(listener).sendActionEvents(actionsCaptor.capture());
        actionStore.populateRecommendedActions(firstPlan);
        final Collection<ActionView> actions = actionsCaptor.getValue();
        Assert.assertEquals(ActionState.READY, ((Action)((ArrayList)actions).get(0)).getState());
    }

    /**
     * Test that we don't remove actions with following states (QUEUED, PRE_IN_PROGRESS,
     * IN_PROGRESS, POST_IN_PROGRESS) from action store.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testPopulateInProgressAreNotCleared() throws Exception {
        final ActionDTO.Action queuedMove = move(vm1, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action inProgressMove = move(vm2, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action preInProgressMove = move(vm3, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action postInProgressMove = move(vm4, hostA, vmType, hostB, vmType).build();

        final ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAllAction(Arrays.asList(queuedMove, inProgressMove, preInProgressMove, postInProgressMove))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);
        when(actionStore.getAction(preInProgressMove.getId()).get().getState()).thenReturn(ActionState.PRE_IN_PROGRESS);
        when(actionStore.getAction(postInProgressMove.getId()).get().getState()).thenReturn(ActionState.POST_IN_PROGRESS);

        final ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(4, actionStore.size());
    }

    /**
     * Test that we don't duplicate actions with following states (QUEUED, PRE_IN_PROGRESS,
     * IN_PROGRESS, POST_IN_PROGRESS) in action store.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testPopulateInProgressNotDuplicated() throws Exception {
        final ActionDTO.Action queuedMove = move(vm1, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action inProgressMove = move(vm2, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action preInProgressMove = move(vm3, hostA, vmType, hostB, vmType).build();
        final ActionDTO.Action postInProgressMove = move(vm4, hostA, vmType, hostB, vmType).build();

        final ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAllAction(Arrays.asList(queuedMove, preInProgressMove, inProgressMove,
                    postInProgressMove))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(queuedMove.getId()).get().getState()).thenReturn(ActionState.QUEUED);
        when(actionStore.getAction(inProgressMove.getId()).get().getState()).thenReturn(ActionState.IN_PROGRESS);
        when(actionStore.getAction(preInProgressMove.getId()).get().getState()).thenReturn(ActionState.PRE_IN_PROGRESS);
        when(actionStore.getAction(postInProgressMove.getId()).get().getState()).thenReturn(ActionState.POST_IN_PROGRESS);

        final ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAllAction(Arrays.asList(queuedMove, preInProgressMove, inProgressMove,
                    postInProgressMove))
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(4, actionStore.size());
        assertThat(actionStore.getActionView(queuedMove.getId()).isPresent(), is(true));
        assertThat(actionStore.getActionView(preInProgressMove.getId()).isPresent(), is(true));
        assertThat(actionStore.getActionView(inProgressMove.getId()).isPresent(), is(true));
        assertThat(actionStore.getActionView(postInProgressMove.getId()).isPresent(), is(true));
    }

    @Test
    public void testPopulateClearedSucceededFailedAreRemoved() throws Exception {
        ActionDTO.Action.Builder successMove =
            move(vm3, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder failedMove =
            move(vm1, hostB, vmType, hostC, vmType);
        ActionDTO.Action.Builder clearedMove =
            move(vm2, hostC, vmType, hostD, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(successMove)
            .addAction(failedMove)
            .addAction(clearedMove)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(successMove.getId()).get().getState()).thenReturn(ActionState.SUCCEEDED);
        when(actionStore.getAction(failedMove.getId()).get().getState()).thenReturn(ActionState.FAILED);
        when(actionStore.getAction(clearedMove.getId()).get().getState()).thenReturn(ActionState.CLEARED);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(0, actionStore.size());
    }

    /**
     * Tests that if the action store receives an action, and it finds out that the action was
     * already successfully executed, it will drop it, unless it is a repeatable action.
     *
     * @throws Exception any exception
     */
    @Test
    public void testClearReadyActionThatAlreadySucceded() throws Exception {
        ActionDTO.Action.Builder successMove =
            move(vm3, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder successProvision = provision(pod1,
                EntityType.CONTAINER_POD_VALUE);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(successMove)
            .addAction(successProvision)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        when(actionStore.getAction(successMove.getId()).get().getState()).thenReturn(ActionState.SUCCEEDED);
        when(actionStore.getAction(successProvision.getId()).get().getState()).thenReturn(ActionState.SUCCEEDED);

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .addAction(successMove)
            .addAction(successProvision)
            .setId(secondPlanId)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertEquals(1, actionStore.size());
        assertThat(actionStore.getActionViews().getAll()
                        .map(ActionView::getRecommendation)
                        .map(ActionDTO.Action::getInfo)
                        .collect(Collectors.toList()),
                containsInAnyOrder(successProvision.getInfo()));
    }

    @Test
    public void testPopulateOneDuplicateReRecommended() throws Exception {
        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(1, actionStore.size());
    }

    @Test
    public void testPopulateReRecommendedWithAdditionalDuplicate() throws Exception {
        ActionDTO.Action.Builder firstMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();

        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        actionStore.populateRecommendedActions(secondPlan);
        assertEquals(3, actionStore.size());
        assertTrue(actionStore.getAction(firstMove.getId()).isPresent());
        assertTrue(actionStore.getAction(secondMove.getId()).isPresent());
    }

    @Test
    public void testPopulation() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        assertThat(actionStore.getActionViews().getAll()
                .map(spec -> spec.getRecommendation().getId())
                .collect(Collectors.toList()),
            containsInAnyOrder(firstMove.getId(), secondMove.getId()));
    }

    /**
     * Tests that if the action store receives an action, and it finds out that the action was
     * already successfully executed within the past hour, it will drop it, unless it is a
     * repeatable action.
     */
    @Test
    public void testPopulateRecommendedActionsRemoveExecutedActions() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove = move(vm2, hostB, vmType, hostC, vmType);
        ActionDTO.Action.Builder thirdMove = move(vm3, hostC, vmType, hostA, vmType);
        ActionDTO.Action.Builder provision = provision(pod1, EntityType.CONTAINER_POD_VALUE);

        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .addAction(thirdMove)
            .addAction(provision)
            .build();

        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        final long secondOid = actionIdentityService.getOidsForObjects(
                Collections.singletonList(secondMove.getInfo())).iterator().next();
        final long provisionOid = actionIdentityService.getOidsForObjects(
                Collections.singletonList(provision.getInfo())).iterator().next();
        final List<Action> filteredActions = Arrays.asList(
                spy(new Action(secondMove.build(), 1L, actionModeCalculator, secondOid)),
                spy(new Action(provision.build(), 1L, actionModeCalculator, provisionOid)));
        filteredActions.forEach(action -> when(action.getState()).thenReturn(ActionState.SUCCEEDED));
        when(actionHistoryDao.getActionHistoryByDate(any(), any()))
                .thenReturn(new ArrayList<>(filteredActions));
        actionStore.populateRecommendedActions(plan);
        assertEquals(3, actionStore.size());
        assertThat(actionStore.getActionViews().getAll()
                .map(spec -> spec.getRecommendation().getInfo())
                .collect(Collectors.toList()),
            containsInAnyOrder(firstMove.getInfo(), thirdMove.getInfo(), provision.getInfo()));
    }

    @Test
    public void testGetActionViews() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder secondMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(firstMove)
            .addAction(secondMove)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        assertThat(actionStore.getActionViews().getAll()
                .map(spec -> spec.getRecommendation().getId())
                .collect(Collectors.toList()),
            containsInAnyOrder(firstMove.getId(), secondMove.getId()));
    }

    @Test
    public void testClearThrowsIllegalStateException() {
        expectedException.expect(IllegalStateException.class);
        actionStore.clear();
    }

    @Test
    public void testGetTopologyContextId() {
        assertEquals(TOPOLOGY_CONTEXT_ID, actionStore.getTopologyContextId());
    }

    @Test
    public void testGetEntitySettings() throws Exception {
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move(vm1, hostA, vmType, hostB, vmType))
            .build();

        actionStore.populateRecommendedActions(plan);

        verify(entitySettingsCache).newSnapshot(eq(ImmutableSet.of(vm1, hostA, hostB)),
            any(),
            eq(plan.getInfo().getMarket().getSourceTopologyInfo().getTopologyContextId()),
            eq(plan.getInfo().getMarket().getSourceTopologyInfo().getTopologyId()));
        verify(spyActionFactory).newAction(any(),
            eq(firstPlanId), Mockito.anyLong());
        assertEquals(1, actionStore.size());
    }

    @Test
    public void testPurgeOfNonRecommendedAction() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        actionStore.populateRecommendedActions(firstPlan);
        Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        when(queuedAction.get().getState()).thenReturn(ActionState.QUEUED);
        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(), is(ActionState.QUEUED));

        ActionDTO.Action.Builder queuedMoveSameSrc =
            move(vm1, hostA, vmType, hostC, vmType);
        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(queuedMoveSameSrc)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(secondPlan);

        assertThat (actionStore.size(), is(2));
        // The 1st action should have received a NotRecommendedEvent.
        verify(actionStore.getAction(queuedMove.getId()).get()).receive(isA(NotRecommendedEvent.class));
        // 2nd one should be in READY state.
        assertThat(actionStore.getAction(queuedMoveSameSrc.getId()).get().getState(), is(ActionState.READY));
    }

    @Test
    public void testTranslationOfRecommendedActions() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        final Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        assertTrue(queuedAction.isPresent());
        // Translation should have succeeded.
        assertThat(queuedAction.get().getTranslationStatus(), is(TranslationStatus.TRANSLATION_SUCCEEDED));
    }

    @Test
    public void testDropFailedTranslations() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan plan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();

        doAnswer(invocation -> {
            Stream<Action> actionStream = (Stream<Action>)invocation.getArgumentAt(0, Stream.class);
            return actionStream.peek(action -> action.getActionTranslation().setTranslationFailure());
        }).when(actionTranslator).translate(any(Stream.class), any(EntitiesAndSettingsSnapshot.class));
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(plan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(plan);
        final Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
        // Translation should have failed, so the action shouldn't be in the store..
        assertFalse(queuedAction.isPresent());
    }

    @Captor
    private ArgumentCaptor<Stream<Action>> translationCaptor;

    /** Defining a Mockito rule to allow initializating the argument captors.
     */
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Captor
    private ArgumentCaptor<Collection<ActionView>> actionsCaptor;

    @Test
    public void testRetentionOfReRecommendedAction() throws Exception {
        ActionDTO.Action.Builder queuedMove =
            move(vm1, hostA, vmType, hostB, vmType);

        ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(queuedMove)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);
        Optional<Action> queuedAction = actionStore.getAction(queuedMove.getId());
//        queuedAction.get().receive(new AutomaticAcceptanceEvent("foo", 123L));
        when(queuedAction.get().getState()).thenReturn(ActionState.QUEUED);
//        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(),
//            is(ActionState.QUEUED));

        ActionDTO.Action.Builder queuedMoveReRecommended =
            move(vm1, hostA, vmType, hostB, vmType);
        ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(queuedMoveReRecommended)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertThat(actionStore.size(), is(1));
        assertThat(actionStore.getAction(queuedMove.getId()).get().getState(),
            is(ActionState.QUEUED));
    }

    @Test
    public void testUpdateOfReRecommendedAction() throws Exception {
        final ActionDTO.Action.Builder move = move(vm1, hostA, vmType, hostB, vmType)
            // Initially the importance is 1 and executability is "false".
            .setDeprecatedImportance(1)
            .setExecutable(false);

        final ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(firstPlanId)
            .addAction(move)
            .build();
        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);

        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getDeprecatedImportance(),
            is(move.getDeprecatedImportance()));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getExecutable(),
            is(move.getExecutable()));

        final ActionDTO.Action.Builder updatedMove = move(vm1, hostA, vmType, hostB, vmType)
            .setDeprecatedImportance(2)
            .setExecutable(true);
        final ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(topologyId))))
            .setId(secondPlanId)
            .addAction(updatedMove)
            .build();
        actionStore.populateRecommendedActions(secondPlan);

        assertThat(actionStore.size(), is(1));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getDeprecatedImportance(),
            is(updatedMove.getDeprecatedImportance()));
        assertThat(actionStore.getAction(move.getId()).get().getRecommendation().getExecutable(),
            is(updatedMove.getExecutable()));
    }

    /**
     * Test a host going into maintenance (host a) and a host going back into powered_on state
     * (host c). We expect that when their state get updated all moving IN actions from host a and
     * all moving OUT actions for host c get cleared. These actions should get cleared in all the
     * subsequent plans until we receive a topology with a most recent id than the state change
     * event.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testHostsWithNewState() throws Exception {
        final ActionDTO.Action.Builder moveInAction = move(vm1, hostA, vmType, hostB, vmType)
            .setDeprecatedImportance(1)
            .setExecutable(false);
        final ActionDTO.Action.Builder moveOutAction = move(vm2, hostC, vmType, hostA, vmType)
            .setDeprecatedImportance(1)
            .setExecutable(false);

        final ActionPlan firstPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(1))))
            .setId(firstPlanId)
            .addAction(moveInAction)
            .addAction(moveOutAction)
            .build();

        final EntitiesAndSettingsSnapshot snapshot =
            entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(firstPlan.getActionList()),
                    Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);

        actionStore.populateRecommendedActions(firstPlan);

        // Changes with this id should be cached until we receive a plan originated by a topology
        // with a bigger or equal id
        final long stateChangeId = 3;
        EntitiesWithNewState entitiesWithNewState = EntitiesWithNewState.newBuilder()
            .setStateChangeId(stateChangeId)
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostB).setEntityState(EntityState.MAINTENANCE)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostC).setEntityState(EntityState.POWERED_ON)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .build();
        actionStore.updateActionsBasedOnNewStates(entitiesWithNewState);

        assertFalse(actionStore.getAction(moveInAction.getId()).isPresent());
        assertFalse(actionStore.getAction(moveOutAction.getId()).isPresent());


        final ActionPlan secondPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(2))))
            .setId(firstPlanId)
            .addAction(moveInAction)
            .addAction(moveOutAction)
            .build();

        actionStore.populateRecommendedActions(secondPlan);
        assertFalse(actionStore.getAction(moveInAction.getId()).isPresent());
        assertFalse(actionStore.getAction(moveOutAction.getId()).isPresent());

        final ActionPlan thirdPlan = ActionPlan.newBuilder()
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(4))))
            .setId(firstPlanId)
            .addAction(moveInAction)
            .addAction(moveOutAction)
            .build();

        actionStore.populateRecommendedActions(thirdPlan);
        assertTrue(actionStore.getAction(moveInAction.getId()).isPresent());
        assertTrue(actionStore.getAction(moveOutAction.getId()).isPresent());
    }


    @Test
    public void testRecommendationTracker() {
        ActionDTO.Action move1 =
            move(vm1, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move2 =
            move(vm2, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move3 =
            move(vm1, hostA, vmType, hostC, vmType).build();
        // Add some duplicates actionInfos to fill the queue with more than 1 entry
        ActionDTO.Action move4 =
            move(vm1, hostA, vmType, hostB, vmType).build();
        ActionDTO.Action move5 =
            move(vm2, hostA, vmType, hostB, vmType).build();

        ActionFactory actionFactory = new ActionFactory(actionModeCalculator);
        List<Action> actions = ImmutableList.of(move1, move2, move3, move4, move5)
            .stream()
            .map(action -> actionFactory.newAction(action, firstPlanId, 334L))
            .collect(Collectors.toList());

        // Now test the recommendation tracker structure.
        // Run many iterations where a different action is taken from the tracker in each iteration
        // to cover various cases (i.e. remove from front, middle, end)
        int numIterations = actions.size();
        for (int i=0; i < numIterations; i++) {
            RecommendationTracker recommendations = new RecommendationTracker();
            actions.forEach(action -> recommendations.add(action));
            Action actionToRemove = actions.get(i);
            recommendations.take(334L);
            Set<Long> actionIdsRemaining =
                actions.stream()
                    .map(a -> a.getId())
                    .filter(id -> id!=actionToRemove.getId())
                    .collect(Collectors.toSet());
            assertThat(actionIdsRemaining, new ReflectionEquals(
                StreamSupport.stream(recommendations.spliterator(), false)
                    .map(action -> action.getId())
                    .collect(Collectors.toSet())));
        }
    }

    /**
     * Set the atomic action specs used to create atomic actions.
     */
    public void setUpActionMergeCache() {
        aggregateEntity1 = ActionEntity.newBuilder()
                .setType(EntityType.WORKLOAD_CONTROLLER_VALUE).setId(controller1)
                .build();

        deDupEntity1 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_SPEC_VALUE).setId(containerSpec1)
                .build();
        deDupEntity2 = ActionEntity.newBuilder()
                .setType(EntityType.CONTAINER_SPEC_VALUE).setId(containerSpec2)
                .build();

        AtomicActionSpec spec1 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(container1, container2))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                                    .setEntity(aggregateEntity1)
                                    .setEntityName("controller1"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                                .setDeDuplicationTarget(AtomicActionEntity.newBuilder()
                                        .setEntity(deDupEntity1)
                                        .setEntityName("spec1"))
                                .addCommodityData(CommodityMergeData.newBuilder()
                                                    .setCommodityType(CommodityType.VCPU))
                                .addCommodityData(CommodityMergeData.newBuilder()
                                                    .setCommodityType(CommodityType.VMEM)))
                .build();

        AtomicActionSpec spec2 = AtomicActionSpec.newBuilder()
                .addAllEntityIds(Arrays.asList(container3, container4))
                .setAggregateEntity(AtomicActionEntity.newBuilder()
                        .setEntity(aggregateEntity1)
                        .setEntityName("controller1"))
                .setResizeSpec(ResizeMergeSpec.newBuilder()
                        .setDeDuplicationTarget(AtomicActionEntity.newBuilder()
                                .setEntity(deDupEntity2)
                                .setEntityName("spec2"))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VCPU))
                        .addCommodityData(CommodityMergeData.newBuilder()
                                .setCommodityType(CommodityType.VMEM)))
                .build();

        List<AtomicActionSpec> resizeSpecs = Arrays.asList(spec1, spec2);

        Map<ActionType, List<AtomicActionSpec>> mergeSpecsInfoMap = new HashMap<>();
        mergeSpecsInfoMap.put(ActionType.RESIZE, resizeSpecs);
        atomicActionSpecsCache.updateAtomicActionSpecsInfo(mergeSpecsInfoMap);

        atomicActionTargetEntities =
                Arrays.asList(aggregateEntity1.getId(), deDupEntity1.getId(), deDupEntity2.getId());
        actionPlanTargetEntities =
                Arrays.asList(container1, container3, container3, container4);


        actionPlanInfo = ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                        .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                                .setTopologyId(topologyId)))
                .build();

        Set<Long> allEntities = Stream.of(atomicActionTargetEntities, actionPlanTargetEntities)
                .flatMap(Collection::stream).collect(Collectors.toSet());

        final EntitiesAndSettingsSnapshot snapshot =
                entitySettingsCache.newSnapshot(allEntities,
                        Collections.emptySet(), TOPOLOGY_CONTEXT_ID, topologyId);
        when(entitySettingsCache.newSnapshot(any(), anySet(), anyLong(), anyLong())).thenReturn(snapshot);
    }

    private static ActionDTO.Action.Builder resize(long targetId) {
        return ActionOrchestratorTestUtils.createResizeRecommendation(IdentityGenerator.next(),
                targetId, CommodityDTO.CommodityType.VCPU, 1.0, 2.0).toBuilder();
    }

    private static ActionDTO.Action.Builder resize(long targetId, CommodityDTO.CommodityType commType,
                                                   final double oldCapacity,
                                                   final double newCapacity) {
        return ActionOrchestratorTestUtils.createResizeRecommendation(IdentityGenerator.next(),
                targetId, commType, oldCapacity, newCapacity).toBuilder();
    }

    /**
     * Test creation of atomic actions.
     * Action plan contains one move and two resizes for container1::VCPU, container1::VCPU
     * Two resize actions are first de-duplicated and then merged to a single atomic action.
     * This creates two additional action DTOs for the LiveActionStore
     * - one non-executable action dto for the de-duplication entity for UI visibility
     * - one executable action dto for the aggregation entity that will execute the action
     * The market actions are deleted after creation of merged actions.
     *
     * @throws Exception thrown by the LiveActionStore if the current thread has been interrupted
     */
    @Test
    public void testPopulateWithAtomicActions() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder resize1 = resize(container1);
        ActionDTO.Action.Builder resize2 = resize(container2);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(firstPlanId)
                .addAction(firstMove)
                .addAction(resize1).addAction(resize2)
                .build();

        actionStore.populateRecommendedActions(firstPlan);

        assertEquals(3, actionStore.size());
    }

    /**
     * Test that atomic actions that are not re-created, when the resizes are disabled in the
     * subsequent market action plans, are removed from the action store.
     *
     * @throws Exception thrown by the LiveActionStore if the current thread has been interrupted
     */
    @Test
    public void testRemovalOfAtomicActionsForDisabledResizes() throws Exception {
        ActionDTO.Action.Builder firstMove = move(vm1, hostA, vmType, hostB, vmType);
        ActionDTO.Action.Builder resize1 = resize(container1);
        ActionDTO.Action.Builder resize2 = resize(container2);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(firstPlanId)
                .addAction(firstMove)
                .addAction(resize1).addAction(resize2)
                .build();

        actionStore.populateRecommendedActions(firstPlan);

        assertEquals(3, actionStore.size());
        assertActionCount(1, 2);

        ActionPlan secondPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(secondPlanId)
                .build();

        actionStore.populateRecommendedActions(secondPlan);

        // check action count
        assertActionCount(0, 0);
    }

    /**
     * Test atomic resize action OIDs when they contain resizes for multiple de-duplication targets.
     * First plan contains resizes for container1
     * 2 Atomic Resize actions created for targets
     * WC- 31 (CS - 41)
     * CS - 41
     * Second plan contains resizes for container1, container3
     * 3 Atomic Resize actions created for targets
     * WC - 31 (CS - 41, 42)
     * CS - 41
     * CS - 42
     * @throws Exception thrown by the LiveActionStore if the current thread has been interrupted
     */
    @Test
    public void testPopulateRecommendedResizesForMultipleSpecs() throws Exception {
        ActionDTO.Action.Builder resize1 = resize(container1);
        ActionDTO.Action.Builder resize3 = resize(container3);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(firstPlanId)
                .addAction(resize1)
                .build();

        actionStore.populateRecommendedActions(firstPlan);

        // check action count
        assertActionCount(1, 1);

        List<Action> controllerActions = getControllerActions();
        final Long oid1 = controllerActions.get(0).getRecommendationOid();

        List<Action> containerSpecActions = getContainerSpecActions();
        final Long csOid1 = containerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                        .getExecutionTarget().getId() == deDupEntity1.getId())
                .findFirst().get().getRecommendationOid();

        ActionPlan secondPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(secondPlanId)
                .addAction(resize1).addAction(resize3)
                .build();

        actionStore.populateRecommendedActions(secondPlan);

        // check action count
        assertActionCount(1, 2);

        List<Action> secondPlanControllerActions = getControllerActions();
        final Long oid2 = secondPlanControllerActions.get(0).getRecommendationOid();

        List<Action> secondPlanContainerSpecActions = getContainerSpecActions();
        final Long csOid2 = secondPlanContainerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                        .getExecutionTarget().getId() == deDupEntity1.getId())
                .findFirst().get().getRecommendationOid();

        // assert that the recommendation OID for atomic action on the controller created
        // by merging resizes from the first plan
        // is not the same as the one  for the atomic action on the same controller
        // by merging resizes from the second plan
        assertNotEquals(oid1, oid2);

        // assert that the recommendation OID for atomic action on the container spec created
        // by merging resizes from the first plan
        // is not the same as the one  for the atomic action on the same container spec
        // by merging resizes from the second plan
        assertEquals(csOid1, csOid2);
    }

    /**
     * Test atomic resize action OIDs when they contain resizes for multiple commodities belonging
     * to the same de-duplication target.
     * First plan contains resizes for container1::VCPU, container1::VMEM
     * 2 Atomic Resize actions created for targets
     * WC- 31 (CS - 41)
     * CS - 41
     * Second plan contains resizes for container1::VMEM, container1::VCPU
     * 2 Atomic Resize actions created for targets
     * WC- 31 (CS - 41)
     * CS - 41
     * @throws Exception thrown by the LiveActionStore if the current thread has been interrupted
     */
    @Test
    public void testPopulateMultipleCommodityResizesForSameSpecs() throws Exception {
        ActionDTO.Action.Builder resize11 = resize(container1, CommodityDTO.CommodityType.VCPU,
                                                        1.0, 2.0);
        ActionDTO.Action.Builder resize12 = resize(container1, CommodityDTO.CommodityType.VMEM,
                                                                    1024, 2048);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(firstPlanId)
                .addAction(resize11).addAction(resize12)
                .build();

        actionStore.populateRecommendedActions(firstPlan);
        // check action count
        assertActionCount(1, 1);

        List<Action> controllerActions = getControllerActions();
        final Long oid1 = controllerActions.get(0).getRecommendationOid();

        List<Action> containerSpecActions = getContainerSpecActions();
        final Long csOid1 = containerSpecActions.get(0).getRecommendationOid();

        ActionPlan secondPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(secondPlanId)
                .addAction(resize12).addAction(resize11)
                .build();

        actionStore.populateRecommendedActions(secondPlan);
        // check action count
        assertActionCount(1, 1);

        List<Action> secondPlanControllerActions = getControllerActions();
        final Long oid2 = secondPlanControllerActions.get(0).getRecommendationOid();

        List<Action> secondPlanContainerSpecActions = getContainerSpecActions();
        final Long csOid2 = secondPlanContainerSpecActions.get(0).getRecommendationOid();

        // assert that the recommendation OID for atomic action on the controller created
        // by merging resizes from the first plan
        // is the same as the one for the atomic action on the same controller
        // by merging resizes from the second plan, even if the resizes were in different order
        assertEquals(oid1, oid2);

        // Same is true for the atomic actions on the container spec
        assertEquals(csOid1, csOid2);
    }

    /**
     * Test atomic resize action OIDs when the resizes belonging to multiple de-duplication targets
     * are re-recommended in different order.
     * First plan contains resizes for or container1::VCPU, container3::VCPU
     * 3 Atomic Resize actions created for targets
     * WC- 31 (CS - 41, 42)
     * CS - 41
     * CS - 42
     * Second plan contains resizes for container3::VCPU, container1::VCPU
     * 3 Atomic Resize actions created for targets
     * WC- 31 (CS - 42,41)
     * CS - 41
     * CS - 42
     * @throws Exception thrown by the LiveActionStore if the current thread has been interrupted
     */
    @Test
    public void testPopulateRecommendedResizesInDifferentOrder() throws Exception {
        ActionDTO.Action.Builder resize1 = resize(container1);
        ActionDTO.Action.Builder resize3 = resize(container3);

        ActionPlan firstPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(firstPlanId)
                .addAction(resize1).addAction(resize3)
                .build();

        actionStore.populateRecommendedActions(firstPlan);

        // check action count
        assertActionCount(1, 2);

        List<Action> controllerActions = getControllerActions();
        final Long oid1 = controllerActions.get(0).getRecommendationOid();

        List<Action> containerSpecActions = getContainerSpecActions();
        final Long csOid1 = containerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                                    .getExecutionTarget().getId() == deDupEntity1.getId())
                .findFirst().get().getRecommendationOid();

        final Long csOid2 = containerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                        .getExecutionTarget().getId() == deDupEntity2.getId())
                .findFirst().get().getRecommendationOid();

        Collection<ActionDTO.Action> actions = new ArrayList<>();
        actions.add(resize3.build());
        actions.add(resize1.build());
        ActionPlan secondPlan = ActionPlan.newBuilder()
                .setInfo(actionPlanInfo)
                .setId(secondPlanId)
                .addAction(resize3).addAction(resize1)
                .build();

        actionStore.populateRecommendedActions(secondPlan);

        // check action count
        assertActionCount(1, 2);

        controllerActions = getControllerActions();
        final Long oid2 = controllerActions.get(0).getRecommendationOid();

        List<Action> secondPlanContainerSpecActions = getContainerSpecActions();
        final Long csOid3 = secondPlanContainerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                        .getExecutionTarget().getId() == deDupEntity1.getId())
                .findFirst().get().getRecommendationOid();

        final Long csOid4 = secondPlanContainerSpecActions.stream()
                .filter(action -> action.getRecommendation().getInfo().getAtomicResize()
                        .getExecutionTarget().getId() == deDupEntity2.getId())
                .findFirst().get().getRecommendationOid();

        // assert that the recommendation OID for atomic action on the controller created
        // by merging resizes from the first plan
        // is the same as the one for the atomic action on the same controller
        // by merging resizes from the second plan, even if the resizes were in different order
        assertEquals(oid1, oid2);

        // Same is true for the atomic actions on both the container specs
        assertEquals(csOid1, csOid3);
        assertEquals(csOid2, csOid4);
    }

    /**
     * Assert action count in the action store.
     *
     * @param actionsForController atomic actions created for controller target
     * @param actionsForContainerSpec atomic actions created for container spec target
     */
    private void assertActionCount(int actionsForController, int actionsForContainerSpec) {
        assertEquals(actionsForController + actionsForContainerSpec, actionStore.size());
    }

    /**
     * Get the atomic actions created for the controller target.
     *
     * @return list of atomic actions for the controler target
     */
    private List<Action> getControllerActions() {
        Map<Long, Action> actions = actionStore.getActions();
        List<Action> controllerActions = actions.entrySet()
                .stream()
                .filter(action -> action.getValue().getRecommendation().getInfo().hasAtomicResize()
                        && action.getValue().getRecommendation()
                        .getInfo().getAtomicResize().getExecutionTarget().getType()
                        == EntityType.WORKLOAD_CONTROLLER_VALUE)
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        return controllerActions;
    }

    private List<Action> getContainerSpecActions() {
        Map<Long, Action> actions = actionStore.getActions();
        List<Action> controllerActions = actions.entrySet()
                .stream()
                .filter(action -> action.getValue().getRecommendation().getInfo().hasAtomicResize()
                        && action.getValue().getRecommendation()
                        .getInfo().getAtomicResize().getExecutionTarget().getType()
                        == EntityType.CONTAINER_SPEC_VALUE)
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        return controllerActions;
    }
}
