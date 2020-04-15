package com.vmturbo.history.listeners;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector.ListExistingPlanIds;

/**
 * Unit tests for {@link HistoryPlanGarbageCollector}.
 */
public class HistoryPlanGarbageCollectorTest {
    private HistorydbIO dbio = mock(HistorydbIO.class);

    private HistoryPlanGarbageCollector garbageCollector = new HistoryPlanGarbageCollector(dbio);

    /**
     * Test listing active ids.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testList() throws Exception {
        when(dbio.listPlansWithStats()).thenReturn(Sets.newHashSet(1L, 2L));

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
        verify(dbio).deletePlanStats(1L);
    }


}