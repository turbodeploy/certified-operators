package com.vmturbo.action.orchestrator.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for the {@link ActionExecutionEntitySelector} interface using the
 * {@link EntityAndActionTypeBasedEntitySelector} class.
 */
public class ActionExecutionEntitySelectorTest {

    /**
     * The class under test
     */
    ActionExecutionEntitySelector entitySelector = new EntityAndActionTypeBasedEntitySelector();

    // A test helper class for building move actions (ActionDTO.Action is a final class and cannot
    // be mocked)
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Test
    public void testVirtualMachineMove() throws UnsupportedActionException {
        long primaryEntityId = 83;
        // Simulate a move action...
        ActionDTO.Action moveAction = testActionBuilder.buildMoveAction(primaryEntityId,
                221,
                EntityType.PHYSICAL_MACHINE_VALUE,
                482,
                EntityType.PHYSICAL_MACHINE_VALUE);

        Optional<ActionEntity> selectedEntityId = entitySelector.getEntity(moveAction);
        assertTrue(selectedEntityId.isPresent());
        assertEquals(primaryEntityId, selectedEntityId.get().getId());
    }

    @Test
    public void testVirtualMachineProvision() throws UnsupportedActionException {
        long primaryEntityId = 102;
        // Simulate a provision action...
        ActionDTO.Action provisionAction =
                testActionBuilder.buildProvisionAction(primaryEntityId, null);

        Optional<ActionEntity> selectedEntityId = entitySelector.getEntity(provisionAction);
        assertTrue(selectedEntityId.isPresent());
        assertEquals(primaryEntityId, selectedEntityId.get().getId());
    }
}
