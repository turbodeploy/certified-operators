package com.vmturbo.action.orchestrator.execution;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionReadinessDetails;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link ActionCombiner}.
 */
public class ActionCombinerTest {

    private static final long VM1_OID = 1L;
    private static final long VM2_OID = 2L;
    private static final long VOLUME1_OID = 3L;
    private static final long VOLUME2_OID = 4L;
    private static final long VOLUME3_OID = 5L;

    private final ActionTopologyStore topologyStore = mock(ActionTopologyStore.class);
    private final ActionModeCalculator actionModeCalculator = mock(ActionModeCalculator.class);

    /**
     * Set up test environment.
     */
    @Before
    public void setUp() {
        // Mock topology with two VMs and three Volumes.
        // Volume1 and Volume2 are attached to VM1.
        // Volume3 is attached to VM2.
        final TopologyGraph<ActionGraphEntity> topologyGraph = mock(TopologyGraph.class);
        final ActionGraphEntity vm1 = mockActionGraphEntity(VM1_OID);
        final ActionGraphEntity vm2 = mockActionGraphEntity(VM2_OID);
        when(topologyGraph.getConsumers(VOLUME1_OID)).thenReturn(Stream.of(vm1));
        when(topologyGraph.getConsumers(VOLUME2_OID)).thenReturn(Stream.of(vm1));
        when(topologyGraph.getConsumers(VOLUME3_OID)).thenReturn(Stream.of(vm2));
        final ActionRealtimeTopology topology = mock(ActionRealtimeTopology.class);
        when(topology.entityGraph()).thenReturn(topologyGraph);
        when(topologyStore.getSourceTopology()).thenReturn(Optional.of(topology));
    }

    /**
     * Test combining disruptive Volume Scale actions related to the same VM. The expected result
     * is that Volume actions should be combined.
     */
    @Test
    public void testDisruptiveActionsOnTheSameVm() {
        // ARRANGE
        final ActionExecutionReadinessDetails action1 = createAction(VOLUME1_OID, 1, true);
        final ActionExecutionReadinessDetails action2 = createAction(VOLUME2_OID, 2, true);
        final ActionExecutionReadinessDetails action3 = createAction(VOLUME3_OID, 3, true);

        // ACT
        final ActionCombiner actionCombiner = new ActionCombiner(topologyStore);
        final Set<Set<ActionExecutionReadinessDetails>> result = actionCombiner.combineActions(
                ImmutableSet.of(action1, action2, action3));

        // ASSERT
        final Set<Set<ActionExecutionReadinessDetails>> expectedResult = ImmutableSet.of(
                ImmutableSet.of(action1, action2),
                Collections.singleton(action3));
        assertEquals(expectedResult, result);
    }

    /**
     * Test combining Volume Scale actions related to the same VM when one of the actions is NOT
     * disruptive. The expected result is that Volume actions should NOT be combined.
     */
    @Test
    public void testNonDisruptiveActionsOnTheSameVm() {
        // ARRANGE
        final ActionExecutionReadinessDetails action1 = createAction(VOLUME1_OID, 1, true);
        final ActionExecutionReadinessDetails action2 = createAction(VOLUME2_OID, 2, false);
        final ActionExecutionReadinessDetails action3 = createAction(VOLUME3_OID, 3, true);

        // ACT
        final ActionCombiner actionCombiner = new ActionCombiner(topologyStore);
        final Set<Set<ActionExecutionReadinessDetails>> result = actionCombiner.combineActions(
                ImmutableSet.of(action1, action2, action3));

        // ASSERT
        final Set<Set<ActionExecutionReadinessDetails>> expectedResult = ImmutableSet.of(
                Collections.singleton(action1),
                Collections.singleton(action2),
                Collections.singleton(action3));
        assertEquals(expectedResult, result);
    }

    private static ActionGraphEntity mockActionGraphEntity(long oid) {
        final ActionGraphEntity entity = mock(ActionGraphEntity.class);
        when(entity.getOid()).thenReturn(oid);
        return entity;
    }

    private ActionExecutionReadinessDetails createAction(
            long volumeId,
            long actionId,
            boolean isDisruptive) {
        final ActionDTO.Action actionDto = ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setInfo(ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(volumeId)
                                        .setType(EntityType.VIRTUAL_VOLUME.getNumber())
                                        .build())
                                .addChanges(ChangeProvider.getDefaultInstance())
                                .build())
                        .build())
                .setDisruptive(isDisruptive)
                // The following fields are only set because they are mandatory
                .setDeprecatedImportance(0D)
                .setExplanation(Explanation.getDefaultInstance())
                .build();
        final Action action = new Action(actionDto, 0, actionModeCalculator, actionId);
        return new ActionExecutionReadinessDetails(action, true, true);
    }
}
