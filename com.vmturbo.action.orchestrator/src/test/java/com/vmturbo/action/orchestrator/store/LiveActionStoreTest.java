package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModel;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator;
import com.vmturbo.action.orchestrator.store.identity.IdentityDataStore;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.identity.InMemoryIdentityStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Integration tests related to the LiveActionStore.
 */
public class LiveActionStoreTest {

    private final long firstPlanId = 0xBEADED;

    private final long vm1 = 1;
    private final long vm2 = 2;
    private final long vm3 = 3;
    private final long vm4 = 4;
    private final long vm5 = 5;

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

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private SpyActionFactory spyActionFactory = spy(new SpyActionFactory());
    private LiveActionStore actionStore;

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

    private EntitySeverityCache entitySeverityCache = mock(EntitySeverityCache.class);
    private WorkflowStore workflowStore = mock(WorkflowStore.class);

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
            entitySettingsCache, actionHistoryDao,
            actionTranslator, clock, userSessionContext,
                licenseCheckClient, acceptedActionsStore, rejectedActionsStore,
                actionIdentityService, involvedEntitiesExpander,
            entitySeverityCache, workflowStore);

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
    }

    private static ActionDTO.Action.Builder move(long targetId,
                                                 long sourceId, int sourceType,
                                                 long destinationId, int destinationType) {
        return ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),
            targetId, sourceId, sourceType, destinationId, destinationType).toBuilder();
    }

    public void setEntitiesOIDs() {
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
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
        when(snapshot.getEntityFromOid(eq(vm5)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm5,
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

}
