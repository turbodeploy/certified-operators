package com.vmturbo.group.setting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector.ListExistingPlanIds;

/**
 * Unit tests for {@link SettingPlanGarbageCollector}.
 */
public class SettingPlanGarbageCollectorTest {
    private SettingStore settingStore = mock(SettingStore.class);

    private SettingPlanGarbageCollector garbageCollector =
            new SettingPlanGarbageCollector(settingStore);

    /**
     * Test listing active ids.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testList() throws Exception {
        Set<Long> planIds = new HashSet<>();
        planIds.add(1L);
        planIds.add(2L);
        when(settingStore.getContextsWithSettings()).thenReturn(planIds);

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
        verify(settingStore).deletePlanSettings(1L);
    }
}
