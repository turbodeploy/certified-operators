package com.vmturbo.action.orchestrator.workflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Unit test for {@link WorkflowDiagnostics}.
 */
public class WorkflowDiagnosticsTest {

    private static final long TGT1 = 100L;
    private static final long TGT2 = 101L;
    private static final long TGT3 = 102L;

    private static final long OID1 = 200L;
    private static final long OID2 = 201L;
    private static final long OID3 = 202L;
    private static final long OID4 = 203L;

    private static final WorkflowInfo INFO_1 = WorkflowInfo.newBuilder()
            .setTargetId(TGT1)
            .setActionPhase(ActionPhase.ON_GENERATION)
            .setName("have a breakfast")
            .setDescription("All students should go and have a breakfast")
            .build();
    private static final WorkflowInfo INFO_2 = WorkflowInfo.newBuilder()
            .setTargetId(TGT2)
            .setActionPhase(ActionPhase.APPROVAL)
            .setName("request approval")
            .setDescription("Request approval for new experiments")
            .build();
    private static final WorkflowInfo INFO_3 = WorkflowInfo.newBuilder()
            .setTargetId(TGT1)
            .setActionPhase(ActionPhase.APPROVAL)
            .setName("request approval (secret)")
            .setDescription("Request approval for secret actions")
            .build();
    private static final Workflow WORKFLOW_1 =
            Workflow.newBuilder().setId(OID1).setWorkflowInfo(INFO_1).build();
    private static final Workflow WORKFLOW_2 =
            Workflow.newBuilder().setId(OID2).setWorkflowInfo(INFO_2).build();
    private static final Workflow WORKFLOW_3 =
            Workflow.newBuilder().setId(OID3).setWorkflowInfo(INFO_3).build();

    private WorkflowStore workflowStore;
    private WorkflowDiagnostics workflowDiagnostics;
    @Captor
    private ArgumentCaptor<List<WorkflowInfo>> workflowCaptor;
    @Captor
    private ArgumentCaptor<Long> targetCaptor;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        workflowStore = Mockito.mock(WorkflowStore.class);
        workflowDiagnostics = new WorkflowDiagnostics(workflowStore);
    }

    /**
     * Test restoring workflows.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testRestore() throws Exception {
        Mockito.when(workflowStore.fetchWorkflows(new WorkflowFilter(Collections.emptyList())))
                .thenReturn(Sets.newHashSet(WORKFLOW_1, WORKFLOW_2, WORKFLOW_3));
        final List<String> diags = collectDiags();
        final Map<Long, List<WorkflowInfo>> persistedWorkflows = restoreDiags(diags);
        Assert.assertEquals(Sets.newHashSet(TGT1, TGT2), persistedWorkflows.keySet());
        Assert.assertEquals(Sets.newHashSet(INFO_1, INFO_3),
                new HashSet<>(persistedWorkflows.get(TGT1)));
        Assert.assertEquals(Collections.singletonList(INFO_2), persistedWorkflows.get(TGT2));
    }

    /**
     * Tests the case when existing set of targets do not match collected set of targets.
     * It is expected that we'll persist empty collection of workflows for every target that
     * is not present in diags.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testOtherTargets() throws Exception {
        final WorkflowInfo info3 = WorkflowInfo.newBuilder(INFO_2).setTargetId(TGT3).build();
        final Workflow workflow3 = Workflow.newBuilder().setId(OID2).setWorkflowInfo(info3).build();
        Mockito.when(workflowStore.fetchWorkflows(new WorkflowFilter(Collections.emptyList())))
                .thenReturn(Sets.newHashSet(WORKFLOW_1, WORKFLOW_2));
        final List<String> diags = collectDiags();
        Mockito.when(workflowStore.fetchWorkflows(new WorkflowFilter(Collections.emptyList())))
                .thenReturn(Sets.newHashSet(WORKFLOW_2, workflow3));
        final Map<Long, List<WorkflowInfo>> persistedWorkflows = restoreDiags(diags);
        Assert.assertEquals(Sets.newHashSet(TGT1, TGT2, TGT3), persistedWorkflows.keySet());
        Assert.assertEquals(Collections.singletonList(INFO_1), persistedWorkflows.get(TGT1));
        Assert.assertEquals(Collections.singletonList(INFO_2), persistedWorkflows.get(TGT2));
        Assert.assertEquals(Collections.emptyList(), persistedWorkflows.get(TGT3));
    }

    @Nonnull
    private Map<Long, List<WorkflowInfo>> restoreDiags(@Nonnull List<String> diags)
            throws WorkflowStoreException, DiagnosticsException {
        workflowDiagnostics.restoreDiags(diags);
        Mockito.verify(workflowStore, Mockito.atLeastOnce())
                .persistWorkflows(targetCaptor.capture(), workflowCaptor.capture());
        final Map<Long, List<WorkflowInfo>> persistedWorkflows = new HashMap<>();
        for (int i = 0; i < targetCaptor.getAllValues().size(); i++) {
            persistedWorkflows.put(targetCaptor.getAllValues().get(i),
                    workflowCaptor.getAllValues().get(i));
        }
        return persistedWorkflows;
    }

    @Nonnull
    private List<String> collectDiags() throws DiagnosticsException {
        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        workflowDiagnostics.collectDiags(appender);
        final ArgumentCaptor<String> diagsCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diagsCaptor.capture());
        return diagsCaptor.getAllValues();
    }
}
