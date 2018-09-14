package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
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
    public WorkflowApiDTO toUiWorkflowApiDTO(@Nonnull Workflow workflow,
                                             @Nonnull TargetApiDTO target) {
        Objects.requireNonNull(workflow);
        Objects.requireNonNull(target);
        WorkflowApiDTO answer = new WorkflowApiDTO();
        answer.setUuid(Long.toString(workflow.getId()));
        WorkflowInfo workflowInfo = workflow.getWorkflowInfo();
        answer.setDisplayName(workflowInfo.getDisplayName());
        answer.setDescription(workflowInfo.getDescription());
        answer.setDiscoveredBy(target);
        // populate the list of parameters for this workflow
        List<InputFieldApiDTO> workflowParameters = workflowInfo.getWorkflowParamList().stream()
                .map(this::convertToInputFieldDTO)
                .collect(Collectors.toList());
        answer.setParameters(workflowParameters);
        // fixed response fields - for compatibility
        answer.setActionType(ActionDTO.ActionType.NONE.toString());
        answer.setClassName(WORKFLOW_API_DTO_CLASSNAME);
        // empty fields not currently used by the Classic implementation
        answer.setEntityType(null);
        return answer;
    }

    /**
     * Convert a WorkflowParameter protobuf message to a corresponding InputFieldApiDTO.
     * Based on the usage in Classic, only the 'name', 'description', and 'type' fields are needed.
     * Further, the "isSecret" field is always set to 'false', and the 'isMandatory' is always
     * set to 'true'.
     *
     * @param param the WorkflowParameter to convert
     * @return an InputFieldApiDTO set from the WorkflowParameter
     */
    private InputFieldApiDTO convertToInputFieldDTO(WorkflowDTO.WorkflowParameter param) {
        InputFieldApiDTO inputFieldDto = new InputFieldApiDTO();
        inputFieldDto.setName(param.getName());
        inputFieldDto.setDescription(param.getDescription());
        inputFieldDto.setSpecificValueType(param.getType());
        inputFieldDto.setIsSecret(false);
        inputFieldDto.setIsMandatory(param.getMandatory());
        return inputFieldDto;
    }
}
