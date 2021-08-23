package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.AuthenticationMethod;
import com.vmturbo.api.dto.workflow.HttpMethod;
import com.vmturbo.api.dto.workflow.RequestHeader;
import com.vmturbo.api.dto.workflow.WebhookApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.OrchestratorType;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper methods between Workflow protobuf and WorkflowApiDTO UI objects.
 **/
public class WorkflowMapper {

    private static final String WORKFLOW_API_DTO_CLASSNAME = "Workflow";

    /**
     * Map an internal Workflow item to the API WorkflowApiDTO, including
     * 'discoveredBy' containing the targetId of the Orchestration target from which
     * the workflow was discovered.
     *
     * @param workflow the internal {@link Workflow} protobuf to be mapped
     * @param target the target from which the workflow was discovered
     * @return a {@link WorkflowApiDTO} for this workflow to return to the UI
     */
    @Nonnull
    public WorkflowApiDTO toWorkflowApiDTO(@Nonnull Workflow workflow,
                                             @Nullable TargetApiDTO target) {
        Objects.requireNonNull(workflow);
        WorkflowApiDTO answer = new WorkflowApiDTO();
        answer.setUuid(Long.toString(workflow.getId()));
        WorkflowInfo workflowInfo = workflow.getWorkflowInfo();
        answer.setDisplayName(workflowInfo.getDisplayName());
        if (workflowInfo.hasDescription()) {
            answer.setDescription(workflowInfo.getDescription());
        }
        answer.setDiscoveredBy(target);
        // populate the list of parameters for this workflow
        List<InputFieldApiDTO> workflowParameters = workflowInfo.getWorkflowParamList().stream()
                .map(this::convertToInputFieldDTO)
                .collect(Collectors.toList());
        answer.setParameters(workflowParameters);
        if (workflowInfo.hasScriptPath()) {
            answer.setScriptPath(workflowInfo.getScriptPath());
        }
        if (workflowInfo.hasEntityType()) {
            answer.setEntityType(EntityType.forNumber(workflowInfo.getEntityType()).name());
        }
        if (workflowInfo.hasActionType()) {
            answer.setActionType(workflowInfo.getActionType().name());
        }
        if (workflowInfo.hasActionPhase()) {
            answer.setActionPhase(workflowInfo.getActionPhase().name());
        }
        if (workflowInfo.hasTimeLimitSeconds()) {
            answer.setTimeLimitSeconds(workflowInfo.getTimeLimitSeconds());
        }
        answer.setType(OrchestratorType.valueOf(workflowInfo.getType().name()));
        if (workflowInfo.getType() == WorkflowDTO.OrchestratorType.WEBHOOK) {
            final WebhookInfo webhookInfo = workflowInfo.getWebhookInfo();
            final WebhookApiDTO webhookApiDTO = new WebhookApiDTO();
            webhookApiDTO.setMethod(HttpMethod.valueOf(webhookInfo.getHttpMethod().name()));
            webhookApiDTO.setUrl(webhookInfo.getUrl());
            if (webhookInfo.hasTemplate()) {
                webhookApiDTO.setTemplate(webhookInfo.getTemplate());
            }
            if (webhookInfo.hasAuthenticationMethod()) {
                webhookApiDTO.setAuthenticationMethod(AuthenticationMethod.valueOf(webhookInfo.getAuthenticationMethod().name()));
            }
            if (webhookInfo.hasUsername()) {
                webhookApiDTO.setUsername(webhookInfo.getUsername());
            }
            if (!webhookInfo.getHeadersList().isEmpty()) {
                webhookApiDTO.setHeaders(webhookInfo.getHeadersList()
                        .stream()
                        .map(el -> new RequestHeader(el.getName(), el.getValue()))
                        .collect(Collectors.toList()));
            }
            webhookApiDTO.setTrustSelfSignedCertificates(webhookInfo.getTrustSelfSignedCertificates());
            answer.setTypeSpecificDetails(webhookApiDTO);
        }
        // fixed response fields - for compatibility
        answer.setClassName(WORKFLOW_API_DTO_CLASSNAME);
        return answer;
    }

    /**
     * Converts a workflow received from UI to the protobuf object used for internal communication.
     *
     * @param workflowApiDTO the UI object for the workflow.
     * @param name the name for workflow.
     * @return the converted object.

     */
    public Workflow fromUiWorkflowApiDTO(@Nonnull WorkflowApiDTO workflowApiDTO, String name) {
        Objects.requireNonNull(workflowApiDTO);
        Workflow.Builder convertedWorkflow = Workflow.newBuilder();
        if (workflowApiDTO.getUuid() != null) {
            convertedWorkflow.setId(Long.parseLong(workflowApiDTO.getUuid()));
        }
        WorkflowInfo.Builder workflowInfo = WorkflowInfo.newBuilder();
        workflowInfo.setName(name);
        workflowInfo.setDisplayName(workflowApiDTO.getDisplayName());
        if (workflowApiDTO.getDescription() != null) {
            workflowInfo.setDescription(workflowApiDTO.getDescription());
        }
        // we don't populate discovered by, parameter fields for workflows coming from ui
        // as there is no use for that right now
        if (workflowApiDTO.getScriptPath() != null) {
            workflowApiDTO.setScriptPath(workflowApiDTO.getScriptPath());
        }
        if (workflowApiDTO.getEntityType() != null) {
            workflowInfo.setEntityType(EntityType.valueOf(workflowApiDTO.getEntityType()).getNumber());
        }
        if (workflowApiDTO.getActionType() != null) {
            workflowInfo.setActionType(ActionDTO.ActionType.valueOf(workflowApiDTO.getActionType()));
        }
        if (workflowApiDTO.getActionPhase() != null) {
            workflowInfo.setActionPhase(ActionDTO.ActionPhase.valueOf(workflowApiDTO.getActionPhase()));
        }
        if (workflowApiDTO.getTimeLimitSeconds() != null) {
            workflowInfo.setTimeLimitSeconds(workflowApiDTO.getTimeLimitSeconds());
        }
        workflowInfo.setType(WorkflowDTO.OrchestratorType.valueOf(workflowApiDTO.getType().name()));

        if (workflowApiDTO.getType() == OrchestratorType.WEBHOOK) {
            WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();
            WebhookInfo.Builder builder = WebhookInfo.newBuilder();
            builder.setUrl(webhookApiDTO.getUrl());
            builder.setHttpMethod(WebhookInfo.HttpMethod.valueOf(webhookApiDTO.getMethod().name()));
            if (webhookApiDTO.getTemplate() != null) {
                builder.setTemplate(webhookApiDTO.getTemplate());
            }
            if (webhookApiDTO.getAuthenticationMethod() != null) {
                builder.setAuthenticationMethod(WebhookInfo.AuthenticationMethod.valueOf(webhookApiDTO.getAuthenticationMethod().name()));
            }
            if (webhookApiDTO.getUsername() != null) {
                builder.setUsername(webhookApiDTO.getUsername());
            }
            Boolean trustSelfSignedCertificates = webhookApiDTO.getTrustSelfSignedCertificates();
            if (trustSelfSignedCertificates != null) {
                builder.setTrustSelfSignedCertificates(trustSelfSignedCertificates);
            }
            final List<RequestHeader> headers = webhookApiDTO.getHeaders();
            if (headers != null) {
                headers.forEach(
                        header -> builder.addHeaders(WebhookInfo.RequestHeader.newBuilder().setName(
                                header.getName()).setValue(header.getValue()).build()));

            }
            workflowInfo.setWebhookInfo(builder);
        }

        convertedWorkflow.setWorkflowInfo(workflowInfo);
        return convertedWorkflow.build();

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