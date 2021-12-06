package com.vmturbo.action.orchestrator.workflow.webhook;

import static com.vmturbo.platform.sdk.common.util.WebhookConstants.TEMPLATED_ACTION_BODY;
import static com.vmturbo.platform.sdk.common.util.WebhookConstants.URL;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.runtime.parser.ParseException;

import com.vmturbo.action.orchestrator.velocity.Velocity;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.api.ApiMessage.ActionConversionRequest;
import com.vmturbo.common.protobuf.api.ApiMessageServiceGrpc.ApiMessageServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.WebhookInfo;
import com.vmturbo.components.api.tracing.Tracing;

/**
 * Manages application of a template on an action.
 */
public class ActionTemplateApplicator {
    private static final Logger logger = LogManager.getLogger();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ApiMessageServiceBlockingStub apiMessageServiceBlockingStub;

    /**
     * Creates an instance {@link ActionTemplateApplicator}.
     *
     * @param apiMessageServiceBlockingStub the stub to call API action conversion service.
     */
    public ActionTemplateApplicator(final ApiMessageServiceBlockingStub apiMessageServiceBlockingStub) {
        this.apiMessageServiceBlockingStub = apiMessageServiceBlockingStub;
    }

    /**
     * Adds template properties to webhook workflow object using input serialized {@link ActionApiDTO}.
     * It will return the input workflow if the type of workflow is not webhook.
     *
     * @param serializedActionApiDTO the serialized {@link ActionApiDTO}.
     * @param workflow the workflow to add templated properties to.
     * @return the workflow with the added properties.
     * @throws ActionTemplateApplicationException if there is an issue with applying template.
     */
    public Workflow addTemplateInformation(String serializedActionApiDTO, Workflow workflow)
            throws ActionTemplateApplicationException {
        if (workflow.getWorkflowInfo().getType() == OrchestratorType.WEBHOOK) {
            WebhookInfo webhookInfo = workflow.getWorkflowInfo().getWebhookInfo();
            final ActionApiDTO actionApiDTO;
            try {
                actionApiDTO = OBJECT_MAPPER.readValue(serializedActionApiDTO, ActionApiDTO.class);
            } catch (JsonProcessingException ex) {
                logger.error("Failed to de-serialize ActionApiDTO: {} ",
                        serializedActionApiDTO, ex);
                throw new ActionTemplateApplicationException("Failed to de-serialize ActionApiDTO.", ex);
            }

            WorkflowInfo.Builder workflowInfoBuilder = WorkflowInfo.newBuilder(workflow.getWorkflowInfo());

            workflowInfoBuilder.addWorkflowProperty(WorkflowDTO.WorkflowProperty.newBuilder()
                    .setName(URL)
                    .setValue(applyTemplate(webhookInfo.getUrl(), actionApiDTO))
                    .build());

            if (webhookInfo.hasTemplate()) {
                workflowInfoBuilder.addWorkflowProperty(WorkflowDTO.WorkflowProperty.newBuilder()
                        .setName(TEMPLATED_ACTION_BODY)
                        .setValue(applyTemplate(webhookInfo.getTemplate(), actionApiDTO))
                        .build());
            }

            return Workflow.newBuilder(workflow)
                    .setWorkflowInfo(workflowInfoBuilder)
                    .build();
        } else {
            return workflow;
        }
    }

    /**
     * Adds template properties to webhook workflow object using input {@link ActionSpec}. It will return the
     * input workflow if the type of workflow is not webhook.
     *
     * @param actionSpec the input {@link ActionSpec}.
     * @param workflow the workflow to add templated properties to.
     * @return the workflow with the added properties.
     * @throws ActionTemplateApplicationException if there is an issue with applying template.
     */
    public Workflow addTemplateInformation(ActionSpec actionSpec, Workflow workflow)
            throws ActionTemplateApplicationException {
        if (workflow.getWorkflowInfo().getType() == OrchestratorType.WEBHOOK) {

            final String serializedAction;
            try (Tracing.TracingScope tracingScope = Tracing.trace("API-object-conversion")) {
                serializedAction = apiMessageServiceBlockingStub.convertActionToApiMessage(
                        ActionConversionRequest
                        .newBuilder()
                        .setActionSpec(actionSpec)
                        .build())
                        .getActionApiDto();
            } catch (StatusRuntimeException ex) {
                logger.error("Failed to access API to convert the action with ID {}.",
                        actionSpec.getRecommendationId(), ex);
                throw new ActionTemplateApplicationException("Failed to access API to convert the action.", ex);
            }

            return addTemplateInformation(serializedAction, workflow);
        } else {
            return workflow;
        }
    }

    private String applyTemplate(String template, ActionApiDTO actionApiDTO)
            throws ActionTemplateApplicationException {
        try (Tracing.TracingScope tracingScope = Tracing.trace("applying_template")) {
            return Velocity.apply(template, actionApiDTO);
        } catch (ParseException | MethodInvocationException | IOException ex) {
            logger.error("Applying webhook template failed for workflow {} because of an Exception.",
                    template, ex);
            throw new ActionTemplateApplicationException("Exception while applying template: " + ex.getMessage(), ex);
        }
    }

    /**
     * Exception while applying template on the action.
     */
    public static class ActionTemplateApplicationException extends Exception {

        /**
         * Creates an instance of {@link ActionTemplateApplicationException}.
         *
         * @param message the exception message.
         * @param cause the cause of exception.
         */
        public ActionTemplateApplicationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
