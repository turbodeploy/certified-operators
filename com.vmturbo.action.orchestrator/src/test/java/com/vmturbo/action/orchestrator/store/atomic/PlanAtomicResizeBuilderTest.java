package com.vmturbo.action.orchestrator.store.atomic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Unit tests for PlanAtomicResizeBuilderTest.
 */
public class PlanAtomicResizeBuilderTest extends AtomicResizeBuilderTest  {

    /**
     * Test that a merged non-executable atomic action is created for all the plan actions.
     * Executable Atomic action is not created.
     */
    @Test
    public void mergeAllActionsInRecommendMode() {
        AggregatedAction aggregatedAction = new AggregatedAction(ActionDTO.ActionInfo.ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));

        AtomicResizeBuilder actionBuilder = new PlanAtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionFactory.AtomicActionResult> atomicActionResult = actionBuilder.build();

        // aggregated atomic action is created
        assertTrue(atomicActionResult.isPresent());

        // Non-executable atomic action
        assertTrue(atomicActionResult.get().nonExecutableAtomicAction().isPresent());

        ActionDTO.Action atomicAction = atomicActionResult.get().nonExecutableAtomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        ActionDTO.AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());

        // executable atomic action is not created
        assertFalse(atomicActionResult.get().atomicAction().isPresent());
    }
}