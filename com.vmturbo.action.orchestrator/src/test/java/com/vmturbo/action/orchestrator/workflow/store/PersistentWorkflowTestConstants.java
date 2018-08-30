package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;

/**
 * Items common to the PersistentWorkflow tests.
 **/
public class PersistentWorkflowTestConstants {
    public static final long TARGET_ID = 111L;
    public static final String WORKFLOW_1_NAME = "workflow-1";
    public static final String WORKFLOW_2_NAME = "workflow-2";
    public static final long WORKFLOW_1_OID = 1L;
    public static final long WORKFLOW_2_OID = 2L;
    public static final long WORKFLOW_1_TARGET_ID = 123L;
    public static final SimpleMatchingAttributes attr1 = SimpleMatchingAttributes.newBuilder()
            .addAttribute(WORKFLOW_NAME, WORKFLOW_1_NAME)
            .addAttribute(WORKFLOW_TARGET_ID, Long.toString(WORKFLOW_1_TARGET_ID))
            .build();
    public static final long WORKFLOW_2_TARGET_ID = 456L;
    public static final WorkflowDTO.WorkflowInfo workflow2 = WorkflowDTO.WorkflowInfo.newBuilder()
            .setTargetId(WORKFLOW_2_TARGET_ID)
            .setName(WORKFLOW_2_NAME)
            .build();
    public static final SimpleMatchingAttributes attr2 = SimpleMatchingAttributes.newBuilder()
            .addAttribute(WORKFLOW_NAME, WORKFLOW_2_NAME)
            .addAttribute(WORKFLOW_TARGET_ID, Long.toString(WORKFLOW_2_TARGET_ID))
            .build();
}
