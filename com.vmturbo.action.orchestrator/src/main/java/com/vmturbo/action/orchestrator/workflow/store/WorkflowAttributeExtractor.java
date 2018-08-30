package com.vmturbo.action.orchestrator.workflow.store;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.identity.attributes.AttributeExtractor;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.attributes.SimpleMatchingAttributes;

/**
 * Extract the matching com.vmturbo.identity.attributes from a WorkflowDTO.WorkflowInfo.
 *
 * These nonVolatile com.vmturbo.identity.attributes are fixed: ("name", item.getName()) and ("targetId", targetId).
 * The volatile and heuristic com.vmturbo.identity.attributes are ignored.
 **/
public class WorkflowAttributeExtractor implements AttributeExtractor<WorkflowDTO.WorkflowInfo> {
    public static final String WORKFLOW_NAME = "name";
    public static final String WORKFLOW_TARGET_ID = "targetId";

    @Override
    @Nonnull
    public IdentityMatchingAttributes extractAttributes(@Nonnull WorkflowDTO.WorkflowInfo item) {
        return SimpleMatchingAttributes.newBuilder()
                .addAttribute(WORKFLOW_NAME, item.getName())
                .addAttribute(WORKFLOW_TARGET_ID, Long.toString(item.getTargetId()))
                .build();
    }
}
