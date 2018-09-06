package com.vmturbo.api.component.external.api.mapper;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;

/**
 * Mapper methods between Workflow protobuf and WorkflowApiDTO UI objects.
 **/
public class WorkflowMapper {

    public static final String WORKFLOW_API_DTO_CLASSNAME = "Workflow";

    /**
     * Map an internal Workflow item to the UI WorkflowApiDTO, including
     * 'discoveredBy' containing the targetId of the Orchestration target from which
     * the workflow was discovered.
     *
     * @param workflow the internal {@link Workflow} protobuf to be mapped
     * @param target the target from which the workflow was discovered
     * @return a {@link WorkflowApiDTO} for this workflow to return to the UI
     */
    @Nonnull
    public static WorkflowApiDTO toUiWorkflowApiDTO(@Nonnull Workflow workflow,
                                                    @Nonnull TargetApiDTO target) {
        Objects.requireNonNull(workflow);
        Objects.requireNonNull(target);
        WorkflowApiDTO answer = new WorkflowApiDTO();
        answer.setUuid(Long.toString(workflow.getId()));
        WorkflowInfo workflowInfo = workflow.getWorkflowInfo();
        answer.setDisplayName(workflowInfo.getDisplayName());
        answer.setDescription(workflowInfo.getDescription());
        answer.setDiscoveredBy(target);
        // fixed response fields
        answer.setActionType(ActionDTO.ActionType.NONE.toString());
        answer.setClassName(WORKFLOW_API_DTO_CLASSNAME);
        return answer;
    }
}
