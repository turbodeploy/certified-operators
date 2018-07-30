package com.vmturbo.topology.processor.workflow;

import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.NME_WITH_WORKFLOWS;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.TARGET_ID;
import static com.vmturbo.topology.processor.workflow.DiscoveredWorkflowTestUtils.EXPECTED_WORKFLOW_DTOS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;

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

}