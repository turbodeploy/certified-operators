package com.vmturbo.topology.processor.actions.data.context;

import static org.hamcrest.core.Is.is;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.ActionExecution;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.actions.ActionExecutionTestUtils;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests the {@code MoveContext} class.
 */
public class MoveContextTest {
    private static final long ACTION_ID = 11L;
    private static final long TARGET_ID = 22L;
    private static final long PROBE_ID = 33L;
    private static final long ENTITY_ID = 44L;
    private static final String PROBE_TYPE = "TheProbe";
    private static final String PROBE_CATEGORY = "ActionOrchestrator";

    @Mock
    private TargetStore targetStore;

    @Mock
    private ProbeStore probeStore;

    @Mock
    private EntityStore entityStore;

    @Mock
    private EntityRetriever entityRetriever;

    @Mock
    private ActionDataManager actionDataManager;

    @Mock
    private Target target;

    private final ActionDTO.ActionInfo actionInfo = ActionDTO.ActionInfo.newBuilder()
        .setMove(ActionDTO.Move.newBuilder()
            .setTarget(ActionExecutionTestUtils.createActionEntity(ENTITY_ID))
        )
        .build();

    private final ActionExecution.ExecuteActionRequest actionRequest =
        ActionExecution.ExecuteActionRequest.newBuilder()
        .setActionId(ACTION_ID)
        .setTargetId(TARGET_ID)
        .setActionInfo(actionInfo)
        .setActionType(ActionDTO.ActionType.RESIZE)
        .setActionState(ActionState.IN_PROGRESS)
        .build();

    /**
     * Initializes the test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(targetStore.getTarget(TARGET_ID)).thenReturn(Optional.of(target));
        Mockito.when(target.getProbeId()).thenReturn(PROBE_ID);
    }

    /**
     * Test the case where we get the sdk action type for a move action execution request when the
     * probe does not support API consistent action types.
     *
     * @throws ContextCreationException if something goes wrong.
     */
    @Test
    public void testGetSDKActionTypeLegacy() throws ContextCreationException {
        ProbeInfo probeInfo = ProbeInfo
            .newBuilder()
            .setProbeType(PROBE_TYPE)
            .setProbeCategory(PROBE_CATEGORY)
            .build();

        Mockito.when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(probeInfo));
        MoveContext context = new MoveContext(actionRequest, actionDataManager, entityStore,
            entityRetriever, targetStore, probeStore);
        Assert.assertThat(context.getSDKActionType(),
            is(ActionType.RIGHT_SIZE));
    }

    /**
     * Test the case where we get the sdk action type for a move action execution request when the
     * probe does support API consistent action types.
     *
     * @throws ContextCreationException if something goes wrong.
     */
    @Test
    public void testGetSDKActionTypeV2() throws ContextCreationException {
        ProbeInfo probeInfo = ProbeInfo
            .newBuilder()
            .setProbeType(PROBE_TYPE)
            .setProbeCategory(PROBE_CATEGORY)
            .setSupportsV2ActionTypes(true)
            .build();

        Mockito.when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(probeInfo));
        MoveContext context = new MoveContext(actionRequest, actionDataManager, entityStore,
            entityRetriever, targetStore, probeStore);
        Assert.assertThat(context.getSDKActionType(),
            is(ActionType.RESIZE));
    }

    /**
     * Tests the case where the lookup the target for the action returns nothing.
     *
     * @throws ContextCreationException expected as the action request does not have a target that
     * we can lookup.
     */
    @Test(expected = ContextCreationException.class)
    public void testGetSDKActionTypeNoTarget() throws ContextCreationException {
        ProbeInfo probeInfo = ProbeInfo
            .newBuilder()
            .setProbeType(PROBE_TYPE)
            .setProbeCategory(PROBE_CATEGORY)
            .setSupportsV2ActionTypes(true)
            .build();
        Mockito.when(targetStore.getTarget(TARGET_ID)).thenReturn(Optional.empty());
        Mockito.when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.of(probeInfo));
        MoveContext context = new MoveContext(actionRequest, actionDataManager, entityStore,
            entityRetriever, targetStore, probeStore);
        context.getSDKActionType();
    }

    /**
     * Tests the case where the lookup the probe for the target related to action returns nothing.
     *
     * @throws ContextCreationException expected as the action request does not have a probe that
     * we can lookup.
     */
    @Test(expected = ContextCreationException.class)
    public void testGetSDKActionTypeNoProbe() throws ContextCreationException {
        Mockito.when(probeStore.getProbe(PROBE_ID)).thenReturn(Optional.empty());
        MoveContext context = new MoveContext(actionRequest, actionDataManager, entityStore,
            entityRetriever, targetStore, probeStore);
        context.getSDKActionType();
    }

}