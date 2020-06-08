package com.vmturbo.topology.processor.workflow;

import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.EXPECTED_WORKFLOW_DTOS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.NME_WITH_WORKFLOWS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.TARGET_ID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;

public class DiscoveredWorkflowInterpreterTest {

    @Test
    public void testInterpretWorkflowList() {
        // arrange
        DiscoveredWorkflowInterpreter workflowInterpreter = new DiscoveredWorkflowInterpreter();
        // act
        List<WorkflowInfo> workflowDTOs = workflowInterpreter.interpretWorkflowList(NME_WITH_WORKFLOWS,
                TARGET_ID);
        // assert
        assertThat(workflowDTOs.size(), equalTo(EXPECTED_WORKFLOW_DTOS.size()));
        assertThat(workflowDTOs, containsInAnyOrder(EXPECTED_WORKFLOW_DTOS.toArray()));
    }

    /**
     * All {@link ActionScriptPhase}s should successfully be converted into
     * {@link com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase}s.
     */
    @Test
    public void testAllActionScriptPhaseConversionsAreSuccessful() {
        for (ActionScriptPhase actionScriptPhase : ActionScriptPhase.values()) {
            testActionScriptPhaseConversionSucceeds(actionScriptPhase);
        }
    }

    private void testActionScriptPhaseConversionSucceeds(ActionScriptPhase actionScriptPhase) {
        Workflow inputWorkflow = Workflow.newBuilder()
            .setId("not used for this test")
            .setDescription("not used for this test")
            .setPhase(actionScriptPhase)
            .build();

        List<WorkflowInfo> workflowInfoResults = new DiscoveredWorkflowInterpreter()
            .interpretWorkflowList(Collections.singletonList(inputWorkflow), 123L);
        Assert.assertEquals(1, workflowInfoResults.size());
        WorkflowInfo workflowInfoResult = workflowInfoResults.get(0);
        Assert.assertNotNull(workflowInfoResult.getActionPhase());
        Assert.assertEquals(actionScriptPhase.name(), workflowInfoResult.getActionPhase().name());
    }
}