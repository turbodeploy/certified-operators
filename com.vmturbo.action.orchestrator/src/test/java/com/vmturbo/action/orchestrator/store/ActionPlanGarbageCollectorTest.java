package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector.ListExistingPlanIds;

/**
 * Tests for {@link ActionPlanGarbageCollector}.
 */
public class ActionPlanGarbageCollectorTest {
    private ActionStorehouse storehouse = mock(ActionStorehouse.class);

    private ActionPlanGarbageCollector garbageCollector = new ActionPlanGarbageCollector(storehouse);

    /**
     * Test listing active ids.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testList() throws Exception {
        final ActionStore store = mock(ActionStore.class);
        when(storehouse.getAllStores())
            .thenReturn(ImmutableMap.of(1L, store, 2L, store));

        final Set<Long> result = new HashSet<>();
        for (ListExistingPlanIds l : garbageCollector.listPlansWithData()) {
            result.addAll(l.getPlanIds());
        }

        assertThat(result, containsInAnyOrder(1L, 2L));
    }

    /**
     * Test delete gets propagated.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDelete() throws Exception {
        garbageCollector.deletePlanData(1L);
        verify(storehouse).deleteStore(1L);
    }

}