package com.vmturbo.cost.component;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.vmturbo.cost.component.entity.cost.PlanProjectedEntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector.ListExistingPlanIds;

/**
 * Unit tests for {@link CostPlanGarbageCollector}.
 */
public class CostPlanGarbageCollectorTest {

    private ActionContextRIBuyStore actionContextRIBuyStore = mock(ActionContextRIBuyStore.class);

    private PlanProjectedEntityCostStore planProjectedEntityCostStore = mock(PlanProjectedEntityCostStore.class);

    private PlanReservedInstanceStore planReservedInstanceStore = mock(PlanReservedInstanceStore.class);

    private CostPlanGarbageCollector garbageCollector =
        new CostPlanGarbageCollector(actionContextRIBuyStore, planProjectedEntityCostStore,
            planReservedInstanceStore);

    /**
     * Test listing active ids.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testList() throws Exception {
        when(actionContextRIBuyStore.getContextsWithData()).thenReturn(Collections.singleton(1L));
        when(planProjectedEntityCostStore.getPlanIds()).thenReturn(Collections.singleton(2L));
        when(planReservedInstanceStore.getPlanIds()).thenReturn(Collections.singleton(3L));

        final Set<Long> result = new HashSet<>();
        for (ListExistingPlanIds l : garbageCollector.listPlansWithData()) {
            result.addAll(l.getPlanIds());
        }

        assertThat(result, containsInAnyOrder(1L, 2L, 3L));
    }

    /**
     * Test delete gets propagated.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDelete() throws Exception {
        garbageCollector.deletePlanData(1L);
        verify(actionContextRIBuyStore).deleteRIBuyContextData(1L);
        verify(planProjectedEntityCostStore).deletePlanProjectedCosts(1L);
        verify(planReservedInstanceStore).deletePlanReservedInstanceStats(1L);
    }

}