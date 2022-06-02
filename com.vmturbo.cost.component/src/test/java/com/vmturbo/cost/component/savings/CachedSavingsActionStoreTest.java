package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.actionOid;
import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.destinationProviderOid;
import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.entityOid;
import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.livenessState;
import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.sourceProviderOid;
import static com.vmturbo.cost.component.savings.CachedSavingsActionStoreTest.ScaleHeaderColumn.startTime;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChangeWindowResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowResponse;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.util.TestUtils;

/**
 * Tests cache methods.
 */
public class CachedSavingsActionStoreTest {
    /**
     * Underlying cache store.
     */
    private final SavingsActionStore savingsActionStore;

    /**
     * List of canned change window results added to cache, that we simulate as returning from AO.
     */
    private final List<ExecutedActionsChangeWindow> changeWindows;

    private static final long vmEntityId1 = 74478470596685L;
    private static final long vmEntityId2 = 74478470596686L;
    private static final long vmEntityId3 = 74478470596687L;

    private static final long vmActionId1 = 144847242227167L;
    private static final long vmActionId2 = 144847242227168L;
    private static final long vmActionId3 = 144847242227169L;

    private static final long providerIdA = 74325311778670L;
    private static final long providerIdB = 74325311778671L;
    private static final long providerIdC = 74325311778672L;
    private static final long providerIdX = 74325311778500L;
    private static final long providerIdY = 74325311778501L;

    private static final long vm1LiveStartTime = 1652916915000L;
    private static final long vm2LiveStartTime = 1652916975000L;
    private static final long vm1ActivationTime = 1652917035000L;
    private static final long vm2DeactivationTime = 1652917095000L;

    /**
     * Mole for AO gRPC simulation.
     */
    private final ActionsServiceMole actionServiceMole = spy(new ActionsServiceMole());

    /**
     * AO gRPC server.
     */
    @Rule
    public GrpcTestServer actionGrpcServer = GrpcTestServer.newServer(actionServiceMole);

    /**
     * Template file with ExecutedActionsChangeWindow protobuf that is populated with values.
     */
    private static final String vmTemplateFilePath = "/savings/change-window-vm.json";

    /**
     * Fields in template file that are populated with test values.
     */
    enum ScaleHeaderColumn {
        actionOid,
        entityOid,
        sourceProviderOid,
        destinationProviderOid,
        startTime,
        livenessState
    }

    /**
     * NEW state for scale action for VM-1 from tier-A to tier-B.
     */
    private static final Map<String, String> vm1ScaleNewTierAtoB = ImmutableMap.of(
            actionOid.name(), String.valueOf(vmActionId1),
            entityOid.name(), String.valueOf(vmEntityId1),
            sourceProviderOid.name(), String.valueOf(providerIdA),
            destinationProviderOid.name(), String.valueOf(providerIdB),
            startTime.name(), "0", // Need to use 0 if not present in the file.
            livenessState.name(), LivenessState.NEW.name()
    );

    /**
     * LIVE state for scale action for VM-1 from tier-B to tier-C.
     */
    private static final Map<String, String> vm1ScaleLiveTierBtoC = ImmutableMap.of(
            actionOid.name(), String.valueOf(vmActionId2),
            entityOid.name(), String.valueOf(vmEntityId1),
            sourceProviderOid.name(), String.valueOf(providerIdB),
            destinationProviderOid.name(), String.valueOf(providerIdC),
            startTime.name(), String.valueOf(vm1LiveStartTime),
            livenessState.name(), LivenessState.LIVE.name()
    );

    /**
     * LIVE state for scale action for VM-3 from tier-X to tier-Y.
     */
    private static final Map<String, String> vm2ScaleLiveTierBtoC = ImmutableMap.of(
            actionOid.name(), String.valueOf(vmActionId3),
            entityOid.name(), String.valueOf(vmEntityId2),
            sourceProviderOid.name(), String.valueOf(providerIdX),
            destinationProviderOid.name(), String.valueOf(providerIdY),
            startTime.name(), String.valueOf(vm2LiveStartTime),
            livenessState.name(), LivenessState.LIVE.name()
    );

    /**
     * Create new instance.
     *
     * @throws IOException Thrown on IO error.
     */
    public CachedSavingsActionStoreTest() throws IOException {
        final ExecutedActionsChangeWindow.Builder vmBuilder11 = ExecutedActionsChangeWindow.newBuilder();
        TestUtils.loadProtobufBuilder(vmTemplateFilePath, vm1ScaleNewTierAtoB, vmBuilder11);

        final ExecutedActionsChangeWindow.Builder vmBuilder12 = ExecutedActionsChangeWindow.newBuilder();
        TestUtils.loadProtobufBuilder(vmTemplateFilePath, vm1ScaleLiveTierBtoC, vmBuilder12);

        final ExecutedActionsChangeWindow.Builder vmBuilder2 = ExecutedActionsChangeWindow.newBuilder();
        TestUtils.loadProtobufBuilder(vmTemplateFilePath, vm2ScaleLiveTierBtoC, vmBuilder2);

        changeWindows = ImmutableList.of(vmBuilder11.build(), vmBuilder12.build(), vmBuilder2.build());
        // We are not really testing the RPC response here, so we return a static list of
        // change windows here. Testing the cache functionality instead.
        actionGrpcServer.start();
        final ActionsServiceBlockingStub actionService =
                ActionsServiceGrpc.newBlockingStub(actionGrpcServer.getChannel());

        final GetActionChangeWindowResponse response = GetActionChangeWindowResponse.newBuilder()
                .addAllChangeWindows(changeWindows)
                .build();
        when(actionServiceMole.getActionChangeWindows(any(GetActionChangeWindowRequest.class)))
                .thenReturn(response);
        when(actionServiceMole.updateActionChangeWindows(any(UpdateActionChangeWindowRequest.class)))
                .thenReturn(UpdateActionChangeWindowResponse.newBuilder().build());

        this.savingsActionStore = new CachedSavingsActionStore(actionService,
                Clock.systemUTC(), 1, 2, 1, 2,
                true);
    }

    /**
     * Verifies init is successful.
     */
    @Test
    public void initialize() throws SavingsException {
        // Initialize cache.
        ((CachedSavingsActionStore)savingsActionStore).initialize(false);

        // Verify store is ready
        assertEquals(3, getCacheSize());
    }

    /**
     * Gets count of actions current in cache.
     *
     * @return Action count.
     * @throws SavingsException Thrown on cache access error.
     */
    private int getCacheSize() throws SavingsException {
        final Set<ExecutedActionsChangeWindow> actions = new HashSet<>();
        actions.addAll(savingsActionStore.getActions(LivenessState.NEW));
        actions.addAll(savingsActionStore.getActions(LivenessState.LIVE));
        return actions.size();
    }

    /**
     * Tests query by action id from cache.
     */
    @Test
    public void getActionById() throws SavingsException {
        initialize();

        final Optional<ExecutedActionsChangeWindow> vmAction1 =
                savingsActionStore.getAction(vmActionId1);
        assertTrue(vmAction1.isPresent());
        // VM1's first action state is NEW, so verify that.
        assertEquals(vmAction1.get().getLivenessState(), LivenessState.NEW);

        final Optional<ExecutedActionsChangeWindow> vmAction2 =
                savingsActionStore.getAction(vmActionId2);
        assertTrue(vmAction2.isPresent());
        // VM1's 2nd action is of LIVE state.
        assertEquals(vmAction2.get().getLivenessState(), LivenessState.LIVE);

        final Optional<ExecutedActionsChangeWindow> vmAction3 =
                savingsActionStore.getAction(vmActionId3);
        assertTrue(vmAction3.isPresent());
        // VM2's action is of LIVE state.
        assertEquals(vmAction3.get().getLivenessState(), LivenessState.LIVE);

        // Make sure we get null back for a non-existent action id.
        final Optional<ExecutedActionsChangeWindow> notExists =
                savingsActionStore.getAction(99999L);
        assertFalse(notExists.isPresent());
    }

    /**
     * Tests getting action by specifying liveness state.
     */
    @Test
    public void getActionByState() throws SavingsException {
        initialize();

        // There is 1 NEW action in cache.
        final Set<ExecutedActionsChangeWindow> newActions = savingsActionStore.getActions(
                LivenessState.NEW);
        assertNotNull(newActions);
        assertEquals(1, newActions.size());
        assertEquals(vmActionId1, newActions.iterator().next().getActionOid());

        // There are 2 LIVE actions in cache.
        final Set<ExecutedActionsChangeWindow> liveActions = savingsActionStore.getActions(
                LivenessState.LIVE);
        assertNotNull(liveActions);
        assertEquals(2, liveActions.size());
        final Set<Long> expectedActionIds = ImmutableSet.of(vmActionId2, vmActionId3);
        final Set<Long> actualActionIds = liveActions.stream()
                .map(ExecutedActionsChangeWindow::getActionOid)
                .collect(Collectors.toSet());
        assertTrue(CollectionUtils.isEqualCollection(expectedActionIds, actualActionIds));
    }

    /**
     * Tests activating action, i.e. making it LIVE.
     */
    @Test
    public void activateAction() throws SavingsException {
        final long entityId = vmEntityId1;

        // Verify initial 3 actions in cache.
        initialize();

        // Verify that an action-1 still exists in cache.
        // This is the NEW action that we will try to make as LIVE by activating it.
        final Optional<ExecutedActionsChangeWindow> newAction =
                savingsActionStore.getAction(vmActionId1);
        assertTrue(newAction.isPresent());
        assertEquals(newAction.get().getLivenessState(), LivenessState.NEW);
        assertEquals(newAction.get().getEntityOid(), entityId);

        // This VM also happens to currently have an existing LIVE action in the cache. Verify it.
        final Optional<ExecutedActionsChangeWindow> liveAction =
                savingsActionStore.getAction(vmActionId2);
        assertTrue(liveAction.isPresent());
        assertEquals(liveAction.get().getLivenessState(), LivenessState.LIVE);
        assertEquals(liveAction.get().getEntityOid(), entityId);

        // Now we try to make actionId1 to LIVE from its old state of NEW. Save the changes.
        savingsActionStore.activateAction(vmActionId1, vm1ActivationTime);
        savingsActionStore.saveChanges();

        // Verify that action-1 which was NEW before, is now LIVE.
        final Optional<ExecutedActionsChangeWindow> activeAction =
                savingsActionStore.getAction(vmActionId1);
        assertTrue(activeAction.isPresent());
        assertEquals(activeAction.get().getLivenessState(), LivenessState.LIVE);
        assertEquals(activeAction.get().getEntityOid(), entityId);
        assertEquals(activeAction.get().getStartTime(), vm1ActivationTime);
    }

    /**
     * Tests deactivating an action, i.e. making it no longer LIVE.
     */
    @Test
    public void deactivateAction() throws SavingsException {
        final long actionId = vmActionId2;

        // Verify initial 3 actions in cache.
        initialize();

        // Verify that an action-2 still exists in cache.
        assertTrue(savingsActionStore.getAction(actionId).isPresent());

        // Now deactivate the currently LIVE action-2, new state doesn't really matter here. Save.
        savingsActionStore.deactivateAction(actionId, vm2DeactivationTime, LivenessState
                .EXTERNAL_MODIFICATION);
        savingsActionStore.saveChanges();

        // Once the action is no longer LIVE, we remove it from the cache, so verify that.
        assertFalse(savingsActionStore.getAction(actionId).isPresent());
    }

    /**
     * Tests notifying on detection of successful action execution.
     */
    @Test
    public void onNewAction() throws SavingsException {
        // Do a couple of refreshes and verify that refresh() returns 0 (meaning nothing refreshed).
        initialize();

        // Verify first that dirty list is empty.
        assertEquals(0, ((CachedSavingsActionStore)savingsActionStore).countDirty());

        // Simulate a couple of new successful action execution notifications.
        savingsActionStore.onNewAction(vmActionId2, vmEntityId1);
        savingsActionStore.onNewAction(vmActionId3, vmEntityId3);

        // Verify dirty count again.
        assertEquals(2, ((CachedSavingsActionStore)savingsActionStore).countDirty());

        // Getting count will trigger sync of dirty actions.
        assertEquals(3, getCacheSize());

        // Verify no more actions are dirty.
        assertEquals(0, ((CachedSavingsActionStore)savingsActionStore).countDirty());
    }

    /**
     * Updates multiple actions in cache to verify they got updated.
     */
    @Test
    public void multiUpdate() throws SavingsException {
        // Load and verify there are 3 initially.
        initialize();

        // Deactivate all 3.
        assertNotNull(savingsActionStore.getAction(vmActionId1));
        savingsActionStore.deactivateAction(vmActionId1, vm2DeactivationTime,
                LivenessState.SUPERSEDED);

        assertNotNull(savingsActionStore.getAction(vmActionId2));
        savingsActionStore.deactivateAction(vmActionId2, vm2DeactivationTime,
                LivenessState.EXTERNAL_MODIFICATION);

        assertNotNull(savingsActionStore.getAction(vmActionId3));
        savingsActionStore.deactivateAction(vmActionId3, vm2DeactivationTime,
                LivenessState.DELETED);

        // Save the state, forcing a multi-action update.
        savingsActionStore.saveChanges();

        // Verify all 3 actions are gone.
        assertFalse(savingsActionStore.getAction(vmActionId1).isPresent());
        assertFalse(savingsActionStore.getAction(vmActionId2).isPresent());
        assertFalse(savingsActionStore.getAction(vmActionId3).isPresent());
    }
}
