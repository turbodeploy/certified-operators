package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapabilities;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.MoveParameters;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeMoles.ProbeActionCapabilitiesServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for class which resolve whether action supported or not.
 */
public class ActionSupportResolverTest {

    private static final long PROBE_ID = 101L;
    private static final long ACTION_ID = 100;

    private ActionExecutor actionExecutor;

    private ProbeActionCapabilitiesServiceMole actionCapabilitiesSvc;

    private GrpcTestServer testServer;

    private ActionSupportResolver filter;
    private IActionFactory actionFactory;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        actionExecutor = Mockito.mock(ActionExecutor.class);
        actionCapabilitiesSvc = Mockito.spy(new ProbeActionCapabilitiesServiceMole());
        testServer = GrpcTestServer.newServer(actionCapabilitiesSvc);
        testServer.start();
        final ActionCapabilitiesStore actionCapabilitiesStore = new ProbeActionCapabilitiesStore(
                ProbeActionCapabilitiesServiceGrpc.newBlockingStub(testServer.getChannel()));
        filter = new ActionSupportResolver(actionCapabilitiesStore, actionExecutor);
        actionFactory = new ActionFactory();
        Mockito.when(actionExecutor.getProbeIdsForActions(Mockito.any()))
                .thenAnswer(new Answer<Object>() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        final Collection<Action> actions =
                                (Collection<Action>)invocation.getArguments()[0];
                        return actions.stream()
                                .collect(Collectors.toMap(Function.identity(), key -> PROBE_ID));
                    }
                });
        Mockito.when(actionCapabilitiesSvc.listProbeActionCapabilities(Mockito.any()))
                .thenReturn(Collections.singletonList(ProbeActionCapabilities.newBuilder()
                        .setProbeId(PROBE_ID)
                        .addActionCapabilities(createActionCapabilities())
                        .build()));
    }

    @After
    public void shutdown() {
        testServer.close();
    }

    private ProbeActionCapability createActionCapabilities() {
        return ProbeActionCapability.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.MOVE)
                        .setActionCapability(ActionCapability.SUPPORTED)
                        .setMove(MoveParameters.newBuilder()
                                .addTargetEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                        .build())
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.MOVE)
                        .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                        .setMove(MoveParameters.newBuilder()
                                .addTargetEntityType(EntityType.STORAGE_VALUE))
                        .build())
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.ACTIVATE)
                        .setActionCapability(ActionCapability.NOT_SUPPORTED)
                        .build())
                .addCapabilityElement(ActionCapabilityElement.newBuilder()
                        .setActionType(ActionType.DEACTIVATE)
                        .setActionCapability(ActionCapability.NOT_EXECUTABLE)
                        .build())
                .build();
    }

    /**
     * Tests move action, which has supported capability for the only change (host). Action is
     * expected to leave supported.
     *
     * @throws Exception if exception s occur
     */
    @Test
    public void testMoveAcrossHostsSupport() throws Exception {
        final Action src = createAction(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(entity(EntityType.VIRTUAL_MACHINE_VALUE, 1L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(entity(EntityType.PHYSICAL_MACHINE_VALUE, 2L))
                                .setSource(entity(EntityType.PHYSICAL_MACHINE_VALUE, 3L))))
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singleton(src));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.SUPPORTED, result.getSupportLevel());
    }

    /**
     * Tests move action, which has not executable capability for the only change (storage). Action
     * is expected to become show only in this case.
     *
     * @throws Exception if exception s occurred
     */
    @Test
    public void testMoveAcrossStoragesSupport() throws Exception {
        final Action src = createAction(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(entity(EntityType.VIRTUAL_MACHINE_VALUE, 1L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(entity(EntityType.STORAGE_VALUE, 2L))
                                .setSource(entity(EntityType.STORAGE_VALUE, 3L))))
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singleton(src));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.SHOW_ONLY, result.getSupportLevel());
    }

    /**
     * Tests move action, which has different capability for the changes (storage and host). Action
     * is expected to have the least capability of the input changes
     *
     * @throws Exception if exception s occurred
     */
    @Test
    public void testMoveAcrossStoragesAndHostsSupport() throws Exception {
        final Action src = createAction(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(entity(EntityType.VIRTUAL_MACHINE_VALUE, 1L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(entity(EntityType.STORAGE_VALUE, 2L))
                                .setSource(entity(EntityType.STORAGE_VALUE, 3L)))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(entity(EntityType.PHYSICAL_MACHINE_VALUE, 4L))
                                .setSource(entity(EntityType.PHYSICAL_MACHINE_VALUE, 5L))))
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singleton(src));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.SHOW_ONLY, result.getSupportLevel());
    }

    /**
     * Tests move action, which has no capability for the specified destination type (datacenter).
     * Action is expected to be reported as unsupported
     *
     * @throws Exception if exception s occurred
     */
    @Test
    public void testMoveUnsupported() throws Exception {
        final Action src = createAction(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(entity(EntityType.VIRTUAL_MACHINE_VALUE, 1L))
                        .addChanges(ChangeProvider.newBuilder()
                                .setDestination(entity(EntityType.DATACENTER_VALUE, 4L))
                                .setSource(entity(EntityType.DATACENTER_VALUE, 5L))))
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singleton(src));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.UNSUPPORTED, result.getSupportLevel());
    }

    @Test
    public void testActivateAction() throws Exception {
        final Action activate = createAction(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                        .setTarget(ActionOrchestratorTestUtils.createActionEntity(PROBE_ID))
                        .build())
                .build());

        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singletonList(activate));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.UNSUPPORTED, result.getSupportLevel());
    }

    @Test
    public void testDeactivateAction() {
        final Action deactivate = createAction(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                        .setTarget(ActionOrchestratorTestUtils.createActionEntity(PROBE_ID))
                        .build())
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singletonList(deactivate));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.SHOW_ONLY, result.getSupportLevel());
    }

    @Test
    public void testActionForAbsentType() {
        final Action action = createAction(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                        .setTarget(entity(EntityType.PHYSICAL_MACHINE_VALUE, 2L)))
                .build());
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singletonList(action));
        Assert.assertEquals(1, resultCollection.size());
        final Action result = resultCollection.iterator().next();
        Assert.assertEquals(SupportLevel.UNSUPPORTED, result.getSupportLevel());
    }

    @Test
    public void testUnsupportedActionTypeCase() {
        final Action unsupportedAction = createAction(ActionInfo.getDefaultInstance());
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("is not supported by capability matchers");
        final Collection<Action> resultCollection =
                filter.resolveActionsSupporting(Collections.singletonList(unsupportedAction));
    }

    /**
     * This is a tricky test to ensure, that we really cover all the action types, available in the
     * market. As a result, no action processing should throw an exception.
     */
    @Test
    public void testAllActionTypeCase() {
        final Collection<Action> actions = Stream.of(ActionTypeCase.values())
                .filter(actionType -> actionType != ActionTypeCase.ACTIONTYPE_NOT_SET)
                .map(this::createAction)
                .collect(Collectors.toList());
        final Collection<Action> resultCollection = filter.resolveActionsSupporting(actions);
        Assert.assertEquals(actions.size(), resultCollection.size());
    }

    private Action createAction(@Nonnull ActionTypeCase actionType) {
        final ActionDTO.Action recommendation = ActionDTO.Action.newBuilder()
                .setInfo(createActionInfo(actionType))
                .setId(1)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        return actionFactory.newAction(recommendation, 1L);
    }

    private ActionInfo createActionInfo(ActionTypeCase actionType) {
        switch (actionType) {
            case MOVE:
                return ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(entity(1, 1L))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setDestination(entity(1, 2L))
                                        .setSource(entity(1, 3L))))
                        .build();
            case RESIZE:
                return ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder().setTarget(entity(1, 1L)))
                        .build();
            case ACTIVATE:
                return ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTarget(entity(1, 1L)))
                        .build();
            case DEACTIVATE:
                return ActionInfo.newBuilder()
                        .setDeactivate(Deactivate.newBuilder().setTarget(entity(1, 1L)))
                        .build();
            case RECONFIGURE:
                return ActionInfo.newBuilder()
                        .setReconfigure(Reconfigure.newBuilder().setTarget(entity(1, 1L)))
                        .build();
            case PROVISION:
                return ActionInfo.newBuilder()
                        .setProvision(Provision.newBuilder().setEntityToClone(entity(1, 1L)))
                        .build();
            default:
                throw new NotImplementedException("Action type " + actionType +
                        " is not configured to be tested. Add it to this method");
        }
    }

    @Nonnull
    private ActionEntity entity(int entityType, long entityId) {
        return ActionEntity.newBuilder().setType(entityType).setId(entityId).build();
    }

    @Nonnull
    private Action createAction(@Nonnull ActionDTO.ActionInfo actionInfo) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setInfo(actionInfo)
                .setId(ACTION_ID)
                .setImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .build();
        return actionFactory.newAction(action, 1L);
    }
}
