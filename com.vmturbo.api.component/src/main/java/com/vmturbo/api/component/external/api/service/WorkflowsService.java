package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.WorkflowMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.target.TargetDetailLevel;
import com.vmturbo.api.dto.workflow.AuthenticationMethod;
import com.vmturbo.api.dto.workflow.OAuthDataApiDTO;
import com.vmturbo.api.dto.workflow.RequestHeader;
import com.vmturbo.api.dto.workflow.WebhookApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowOperationRequestApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowOperationResponseApiDTO;
import com.vmturbo.api.enums.OrchestratorType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.TypeSpecificInfoCase;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;

/**
 * Service Layer to implement the /workflows endpoints.
 **/
public class WorkflowsService implements IWorkflowsService {

    private static final Logger logger = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String HEADER_FIELD_NAME = "name";
    private static final String HEADER_FIELD_VALUE = "value";
    private final WorkflowServiceBlockingStub workflowServiceRpc;
    private final TargetsService targetsService;
    private final WorkflowMapper workflowMapper;
    private final SettingPolicyServiceBlockingStub settingPolicyRpcService;
    private final SecureStorageClient secureStorageClient;
    private final ActionsService actionsService;

    public WorkflowsService(@Nonnull WorkflowServiceBlockingStub workflowServiceRpc,
                            @Nonnull TargetsService targetsService,
                            @Nonnull WorkflowMapper workflowMapper,
                            @Nonnull SettingPolicyServiceBlockingStub settingsPoliciesService,
                            @Nonnull SecureStorageClient secureStorageClient,
                            @Nonnull ActionsService actionsService) {
        this.workflowServiceRpc = Objects.requireNonNull(workflowServiceRpc);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.workflowMapper = Objects.requireNonNull(workflowMapper);
        this.settingPolicyRpcService = Objects.requireNonNull(settingsPoliciesService);
        this.secureStorageClient = Objects.requireNonNull(secureStorageClient);
        this.actionsService = Objects.requireNonNull(actionsService);
    }

    /**
     * {@inheritDoc}
     * @throws IllegalArgumentException if the 'apiWorkflowType' specified is invalid.
     */
    @Override
    @Nonnull
    public List<WorkflowApiDTO> getWorkflows(@Nullable OrchestratorType apiWorkflowType)
            throws IllegalArgumentException {
        List<WorkflowApiDTO> answer = Lists.newArrayList();
        final FetchWorkflowsRequest.Builder fetchWorkflowsBuilder = FetchWorkflowsRequest.newBuilder();
        // should we filter the workflows by Type?
        if (apiWorkflowType != null) {
            fetchWorkflowsBuilder.setOrchestratorType(WorkflowDTO.OrchestratorType
                    .valueOf(apiWorkflowType.name()));
        }
        // fetch the workflows
        WorkflowDTO.FetchWorkflowsResponse workflowsResponse = workflowServiceRpc
                .fetchWorkflows(fetchWorkflowsBuilder.build());
        // set up a map to cache the OID -> TargetApiDTO to reduce the Target lookups
        final Map<Long, TargetApiDTO> targetMap = targetsService.getTargets().stream()
            .collect(Collectors.toMap(
                targetApiDTO -> Long.valueOf(targetApiDTO.getUuid()),
                Function.identity()));

        for (WorkflowDTO.Workflow workflow : workflowsResponse.getWorkflowsList()) {
            final long workflowTargetOid = workflow.getWorkflowInfo().getTargetId();
            // check the local store for the corresponding targetApiDTO
            TargetApiDTO targetApiDTO = targetMap.get(workflowTargetOid);
            if (targetApiDTO == null) {
                if (workflow.getWorkflowInfo().getType() == WorkflowDTO.OrchestratorType.WEBHOOK) {
                    targetApiDTO = new TargetApiDTO();
                } else {
                    // Workflows with no valid target should not be included in the response
                    continue;
                }
            }
            WorkflowApiDTO dto = workflowMapper.toWorkflowApiDTO(workflow, targetApiDTO);
            answer.add(dto);
        }
        return answer;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>
     * We call the {@link WorkflowServiceGrpc} to fetch the data.
     *
     * @throws StatusRuntimeException if the gRPC call fails
     * @throws UnknownObjectException if the target is not found
     */
    @Override
    public WorkflowApiDTO getWorkflowByUuid(@Nonnull String workflowUuid) throws UnknownObjectException {
        Objects.requireNonNull(workflowUuid);
        // fetch the desired workflow
        final WorkflowDTO.Workflow workflow = getWorkflowById(Long.parseLong(workflowUuid));
        // fetch the corresponding target
        String workflowTargetOid = Long.toString(workflow.getWorkflowInfo().getTargetId());
        TargetApiDTO targetApiDTO = targetsService.getTarget(
                workflowTargetOid, TargetDetailLevel.BASIC);
        // map the workflow and the target to {@link WorkflowApiDTO} and return it
        return workflowMapper.toWorkflowApiDTO(workflow, targetApiDTO);
    }

    @Override
    public WorkflowApiDTO addWorkflow(@Nonnull WorkflowApiDTO workflowApiDTO) throws InvalidOperationException {
        if (workflowApiDTO.getType() != OrchestratorType.WEBHOOK) {
            throw new InvalidOperationException("Workflows of type \"" + workflowApiDTO.getType()
                + "\" cannot be added through API.");
        }

        final WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();

        // password must be selected given that the authentication method is also selected.
        if (webhookApiDTO.getAuthenticationMethod() == AuthenticationMethod.BASIC
                && StringUtils.isEmpty(webhookApiDTO.getPassword())) {
            throw new IllegalArgumentException("The \"password\" must not be empty.");
        }

        final WorkflowDTO.Workflow workflow = workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO,
                UUID.randomUUID().toString());
        final WorkflowDTO.Workflow addedWorkflow = workflowServiceRpc.createWorkflow(
            WorkflowDTO.CreateWorkflowRequest
                .newBuilder()
                .setWorkflow(workflow)
                .build())
            .getWorkflow();

        if (webhookApiDTO.getAuthenticationMethod() == AuthenticationMethod.BASIC) {
            try {
                secureStorageClient.updateValue(StringConstants.WEBHOOK_PASSWORD_SUBJECT,
                        Long.toString(addedWorkflow.getId()), webhookApiDTO.getPassword());
            } catch (CommunicationException e) {
                logger.error("Failed to store webhook password credentials.", e);
                workflowServiceRpc.deleteWorkflow(WorkflowDTO.DeleteWorkflowRequest.newBuilder()
                        .setId(addedWorkflow.getId())
                        .build());
                throw new IllegalStateException("Failed to create workflow because of internal issue.");
            }
        }

        return workflowMapper.toWorkflowApiDTO(addedWorkflow, new TargetApiDTO());
    }

    @Override
    public WorkflowApiDTO editWorkflow(@Nonnull String workflowUuid,
                                       @Nonnull WorkflowApiDTO workflowApiDTO) throws UnknownObjectException, InvalidOperationException {
        final long workflowId = Long.parseLong(workflowUuid);
        final WorkflowDTO.Workflow currentWorkflow = getWorkflowById(workflowId);

        verifyWorkflowCanBeModified(currentWorkflow);

        // make sure we are still sending webhook information
        if (workflowApiDTO.getType() != OrchestratorType.WEBHOOK) {
            throw new InvalidOperationException("The type workflow \""
                + currentWorkflow.getWorkflowInfo().getType() + "\" cannot be changed.");
        }

        final WorkflowDTO.Workflow workflow = workflowMapper.fromUiWorkflowApiDTO(workflowApiDTO,
                currentWorkflow.getWorkflowInfo().getName());
        final WorkflowDTO.Workflow updatedWorkflow =  workflowServiceRpc.updateWorkflow(
            WorkflowDTO.UpdateWorkflowRequest
                .newBuilder()
                .setId(workflowId)
                .setWorkflow(workflow)
                .build())
            .getWorkflow();

        final WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();

        if (webhookApiDTO.getAuthenticationMethod() == AuthenticationMethod.BASIC
                && webhookApiDTO.getPassword() != null) {
            try {
                secureStorageClient.updateValue(StringConstants.WEBHOOK_PASSWORD_SUBJECT,
                        Long.toString(workflowId), webhookApiDTO.getPassword());
            } catch (CommunicationException e) {
                logger.error("Failed to store webhook password credentials, "
                        + "reverting back to original workflow.", e);
                workflowServiceRpc.updateWorkflow(
                        WorkflowDTO.UpdateWorkflowRequest
                                .newBuilder()
                                .setId(workflowId)
                                .setWorkflow(currentWorkflow)
                                .build());
                throw new IllegalStateException("Failed to update workflow because of internal issue.");
            }
        }
        return workflowMapper.toWorkflowApiDTO(updatedWorkflow, new TargetApiDTO());
    }

    @Override
    public void deleteWorkflow(@Nonnull String workflowUuid) throws UnknownObjectException,
          InvalidOperationException {
        final long workflowId = Long.parseLong(workflowUuid);
        final WorkflowDTO.Workflow currentWorkflow = getWorkflowById(workflowId);

        verifyWorkflowCanBeModified(currentWorkflow);

        // verify that workflow is not being used by a policy
        List<SettingProto.SettingPolicy> workflows = getAssociatedPolicies(workflowId);

        if (!workflows.isEmpty()) {
            final String policyNames = workflows.stream()
                .map(SettingProto.SettingPolicy::getInfo)
                .map(SettingProto.SettingPolicyInfo::getDisplayName)
                .collect(Collectors.joining(", "));
            throw new InvalidOperationException("The workflow cannot be deleted as it is being used "
                + " in the follow policies: " + policyNames + " .");
        }

        workflowServiceRpc.deleteWorkflow(WorkflowDTO.DeleteWorkflowRequest.newBuilder()
            .setId(workflowId)
            .build());

        if (currentWorkflow.getWorkflowInfo().getTypeSpecificInfoCase() == TypeSpecificInfoCase.WEBHOOK_INFO) {
            try {
                secureStorageClient.deleteValue(StringConstants.WEBHOOK_PASSWORD_SUBJECT, Long.toString(workflowId));
            } catch (CommunicationException e) {
                logger.error("Failed to delete webhook password credentials.", e);
            }
        }
    }

    @Override
    public WorkflowOperationResponseApiDTO performOperation(String workflowUuid,
                                                            WorkflowOperationRequestApiDTO operationRequest)
            throws Exception {
        final long workflowId = Long.parseLong(workflowUuid);

        // get the workflow to make sure it exists
        final WorkflowDTO.Workflow currentWorkflow = getWorkflowById(workflowId);

        if (currentWorkflow.getWorkflowInfo().getType() != WorkflowDTO.OrchestratorType.WEBHOOK) {
            throw new InvalidOperationException("The workflows of type \""
                    + currentWorkflow.getWorkflowInfo().getType() + "\" cannot be tried out.");
        }

        // get the action
        final ActionApiDTO actionApiDTO = actionsService
                .getActionByUuid(String.valueOf(operationRequest.getActionId()), null);

        // make the call to AO
        final WorkflowDTO.ExecuteWorkflowResponse executionResult = workflowServiceRpc
                .executeWorkflow(WorkflowDTO.ExecuteWorkflowRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setActionApiDTO(OBJECT_MAPPER.writeValueAsString(actionApiDTO))
                .build());

        final WorkflowOperationResponseApiDTO responseApiDTO = new WorkflowOperationResponseApiDTO();
        responseApiDTO.setSucceeded(executionResult.getSucceeded());
        responseApiDTO.setDetails(executionResult.getExecutionDetails());

        return responseApiDTO;
    }

    @Override
    public void validateInput(@Nonnull WorkflowApiDTO workflowApiDTO) {
        if (StringUtils.isEmpty(workflowApiDTO.getDisplayName())) {
            throw new IllegalArgumentException("The \"displayName\" field cannot be blank.");
        }

        if (workflowApiDTO.getType() == OrchestratorType.WEBHOOK) {
            if (workflowApiDTO.getScriptPath() != null) {
                throw new IllegalArgumentException("\"scriptPath\" should not be set for webhook"
                    + " workflows.");
            }

            if (CollectionUtils.isNotEmpty(workflowApiDTO.getParameters())) {
                throw new IllegalArgumentException("\"parameters\" should not be set for webhook "
                    + "workflows");
            }

            // if the type of workflow is webhook, the webhook details should be provided
            if (!(workflowApiDTO.getTypeSpecificDetails() instanceof WebhookApiDTO)) {
                throw new IllegalArgumentException("The \"typeSpecificDetails\" field should be of "
                    + "type \"WebhookApiDTO\"");
            }
            final WebhookApiDTO webhookApiDTO = (WebhookApiDTO)workflowApiDTO.getTypeSpecificDetails();

            if (!webhookApiDTO.getUrl().toLowerCase().startsWith("http://")
                  && !webhookApiDTO.getUrl().toLowerCase().startsWith("https://") ) {
                throw new IllegalArgumentException("The \"url\" field should start with \"http://\" or "
                    + " \"https://\".");
            }

            if (webhookApiDTO.getAuthenticationMethod() == AuthenticationMethod.BASIC) {
                // username must be selected given that the authentication method is also selected.
                if (StringUtils.isEmpty(webhookApiDTO.getUsername())) {
                    throw new IllegalArgumentException("The \"username\" must not be empty.");
                }

                // username can't contain a colon: see https://datatracker.ietf.org/doc/html/rfc7617#section-2 page 4
                if (webhookApiDTO.getUsername().contains(":")) {
                    throw new IllegalArgumentException("The \"username\" must not contain a colon.");
                }
            }

            // validate webhook headers if any
            if (webhookApiDTO.getHeaders() != null) {
                for (RequestHeader header : webhookApiDTO.getHeaders()) {
                    validateRequestHeader(header);
                }
            }

            // Validate webhook oAuth data if present
            if (webhookApiDTO.getAuthenticationMethod() == AuthenticationMethod.OAUTH) {
                OAuthDataApiDTO oAuthData = webhookApiDTO.getOAuthData();
                if (oAuthData != null) {
                    validateOAuthData(oAuthData);
                } else {
                    throw new IllegalArgumentException("The input oAuth data is invalid.");
                }
            }
        }
    }

    /**
     * Validate request header.
     *
     * @param header the header
     */
    private static void validateRequestHeader(@Nonnull RequestHeader header) {
        final String headerName = header.getName();
        validateRequestHeaderField(headerName, HEADER_FIELD_NAME);
        validateRequestHeaderField(header.getValue(), HEADER_FIELD_VALUE);

        // Specific validation for header name, because header with empty name doesn't make sense
        // and wouldn't be sent in the request.
        // But header value can be blank (empty string or only whitespaces) and we don't want to restrict it.
        if (StringUtils.isBlank(headerName)) {
            throw new IllegalArgumentException(
                    "Header name shouldn't be empty or contain only whitespace.");
        }
    }

    /**
     * The HTTP header fields (name and value) should follow the generic rules provided in RFC - <a
     * href="https://datatracker.ietf.org/doc/html/rfc822#section-3.1">LEXICAL ANALYSIS OF
     * MESSAGES</a>.
     *
     * @param headerField the header field (name or value)
     * @param headerFieldTypeName "name" or "value" used for having more detailed error
     *         message
     */
    private static void validateRequestHeaderField(final String headerField,
            final String headerFieldTypeName) {
        if (headerField == null) {
            throw new IllegalArgumentException(
                    "Header " + headerFieldTypeName + " must not be null.");
        }
        boolean headerFieldIsAscii = CharMatcher.ascii().matchesAllOf(headerField);
        if (!headerFieldIsAscii) {
            throw new IllegalArgumentException("Header " + headerFieldTypeName
                    + " must be a sequence of printable ASCII characters. \"" + headerField
                    + "\" is wrong header " + headerFieldTypeName);
        }
    }

    /**
     * Validate the oAuth data.
     *
     * @param oAuthData The input oAuth data.
     */
    private static void validateOAuthData(@Nonnull OAuthDataApiDTO oAuthData) {
        final String clientId = oAuthData.getClientId();
        final String clientSecret = oAuthData.getClientSecret();
        final String authorizationServerUrl = oAuthData.getAuthorizationServerUrl();
        final String scope = oAuthData.getScope();

        if (StringUtils.isBlank(clientId)) {
            throw new IllegalArgumentException("The client ID cannot be empty.");
        }

        if (StringUtils.isBlank(clientSecret)) {
            throw new IllegalArgumentException("The client secret cannot be empty.");
        }

        if (StringUtils.isBlank(authorizationServerUrl)) {
            throw new IllegalArgumentException("The authorization server URL cannot be empty.");
        }

        if (!authorizationServerUrl.toLowerCase().startsWith("http://")
            && !authorizationServerUrl.toLowerCase().startsWith("https://") ) {
            throw new IllegalArgumentException("The \"authorization server Url\" must start with \"http://\" or "
                    + " \"https://\".");
        }
    }

    @Nonnull
    private WorkflowDTO.Workflow getWorkflowById(long workflowId) throws UnknownObjectException {
        final WorkflowDTO.FetchWorkflowResponse fetchWorkflowResponse = workflowServiceRpc.fetchWorkflow(
            WorkflowDTO.FetchWorkflowRequest.newBuilder()
                .setId(workflowId)
                .build());
        if (fetchWorkflowResponse.hasWorkflow()) {
            return fetchWorkflowResponse.getWorkflow();
        } else {
            throw new UnknownObjectException("Workflow with id: " + workflowId + " not found");
        }
    }

    private void verifyWorkflowCanBeModified(@Nonnull WorkflowDTO.Workflow currentWorkflow) throws InvalidOperationException {
        if (currentWorkflow.getWorkflowInfo().getType() != WorkflowDTO.OrchestratorType.WEBHOOK) {
            throw new InvalidOperationException("The workflows of type \""
                + currentWorkflow.getWorkflowInfo().getType() + "\" cannot be modified through API.");
        }
    }

    private List<SettingProto.SettingPolicy> getAssociatedPolicies(long workflowId) {
        final List<SettingProto.SettingPolicy> settingPolicies = new ArrayList<>();
        settingPolicyRpcService.listSettingPolicies(SettingProto.ListSettingPoliciesRequest
            .newBuilder()
            .addWorkflowId(workflowId)
            .build()).forEachRemaining(settingPolicies::add);
        return settingPolicies;
    }
}
