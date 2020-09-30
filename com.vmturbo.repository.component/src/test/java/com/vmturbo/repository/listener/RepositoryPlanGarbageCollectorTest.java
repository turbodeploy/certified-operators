package com.vmturbo.repository.listener;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector.ListExistingPlanIds;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;

/**
 * Unit tests for {@link RepositoryPlanGarbageCollector}.
 */
public class RepositoryPlanGarbageCollectorTest {

    private static final long CONTEXT_ID = 1L;
    private static final TopologyID SRC_TID = new TopologyID(CONTEXT_ID, 7, TopologyType.SOURCE);
    private static final TopologyID PROJ_TID = new TopologyID(CONTEXT_ID, 8, TopologyType.PROJECTED);

    private TopologyLifecycleManager lifecycleManager = mock(TopologyLifecycleManager.class);

    private RepositoryPlanGarbageCollector garbageCollector = new RepositoryPlanGarbageCollector(lifecycleManager);

    /**
     * Common setup code before every test.
     */
    @Before
    public void setup() {
        when(lifecycleManager.getTopologyId(CONTEXT_ID, TopologyType.SOURCE))
            .thenReturn(Optional.of(SRC_TID));
        when(lifecycleManager.getTopologyId(CONTEXT_ID, TopologyType.PROJECTED))
            .thenReturn(Optional.of(PROJ_TID));
    }

    /**
     * Test listing active ids.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testList() throws Exception {
        when(lifecycleManager.listRegisteredContexts()).thenReturn(Sets.newHashSet(1L, 2L));

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
        garbageCollector.deletePlanData(CONTEXT_ID);

        verify(lifecycleManager).deleteTopology(SRC_TID);
        verify(lifecycleManager).deleteTopology(PROJ_TID);
    }


    /**
     * Test projected topology not found.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeleteOnlySrcFound() throws Exception {
        when(lifecycleManager.getTopologyId(CONTEXT_ID, TopologyType.PROJECTED))
            .thenReturn(Optional.empty());

        garbageCollector.deletePlanData(CONTEXT_ID);

        verify(lifecycleManager).deleteTopology(SRC_TID);
    }

    /**
     * Test projected topology failed to delete.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeleteOnlySrcSucceed() throws Exception {

        doThrow(new TopologyDeletionException(
            Collections.singletonList("foo"))).when(lifecycleManager).deleteTopology(PROJ_TID);

        garbageCollector.deletePlanData(CONTEXT_ID);

        verify(lifecycleManager).deleteTopology(SRC_TID);
        verify(lifecycleManager).deleteTopology(PROJ_TID);
    }


    /**
     * Test source topology not found.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeleteOnlyProjFound() throws Exception {
        when(lifecycleManager.getTopologyId(CONTEXT_ID, TopologyType.SOURCE))
            .thenReturn(Optional.empty());

        garbageCollector.deletePlanData(CONTEXT_ID);

        verify(lifecycleManager).deleteTopology(PROJ_TID);
    }

    /**
     * Test source failed to delete.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeleteOnlyProjSucceed() throws Exception {

        doThrow(new TopologyDeletionException(
            Collections.singletonList("foo"))).when(lifecycleManager).deleteTopology(SRC_TID);

        garbageCollector.deletePlanData(CONTEXT_ID);

        verify(lifecycleManager).deleteTopology(SRC_TID);
        verify(lifecycleManager).deleteTopology(PROJ_TID);
    }

}