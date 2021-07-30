package com.vmturbo.action.orchestrator.store.atomic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Unit tests for PlanAtomicResizeBuilderTest.
 */
public class PlanAtomicResizeBuilderTest extends AtomicResizeBuilderTest  {

    /**
     * Test that a merged atomic action is created for all the actions that are in RECOMMEND mode.
     * Atomic action is not created for de-duplicated target.
     */
    @Test
    public void mergeAllActionsInRecommendMode() {
        when(view1.getMode()).thenReturn(ActionDTO.ActionMode.RECOMMEND);
        when(view2.getMode()).thenReturn(ActionDTO.ActionMode.RECOMMEND);

        AggregatedAction aggregatedAction = new AggregatedAction(ActionDTO.ActionInfo.ActionTypeCase.ATOMICRESIZE,
                aggregateEntity1.getEntity(),
                aggregateEntity1.getEntityName());
        aggregatedAction.addAction(resize1, Optional.of(deDupEntity1));
        aggregatedAction.addAction(resize2, Optional.of(deDupEntity1));
        aggregatedAction.updateActionView(resize1.getId(), view1);
        aggregatedAction.updateActionView(resize2.getId(), view2);

        AtomicResizeBuilder actionBuilder = new PlanAtomicResizeBuilder(aggregatedAction);
        Optional<AtomicActionFactory.AtomicActionResult> atomicActionResult = actionBuilder.build();

        // aggregated atomic action is created
        assertTrue(atomicActionResult.isPresent());
        assertTrue(atomicActionResult.get().atomicAction().isPresent());

        ActionDTO.Action atomicAction = atomicActionResult.get().atomicAction().get();
        assertTrue(atomicAction.getInfo().hasAtomicResize());

        ActionDTO.AtomicResize resize = atomicAction.getInfo().getAtomicResize();
        assertEquals(aggregateEntity1.getEntity(), resize.getExecutionTarget());
        assertEquals(1, resize.getResizesCount());
        // de-duplicated atomic actions are not created
        assertEquals(0, atomicActionResult.get().deDuplicatedActions().size());
    }
}