package com.vmturbo.action.orchestrator.workflow.rpc;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.CreateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.DeleteWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.UpdateWorkflowResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * gRPC Service for fetching Workflow items from the persistent store.
 **/
public class WorkflowRpcService extends WorkflowServiceGrpc.WorkflowServiceImplBase  {

    private static final String ACTION_STREAM_KAFKA = "ActionStreamKafka";

    // the store for the workflows that are uploaded
    private final WorkflowStore workflowStore;
    private final ThinTargetCache thinTargetCache;

    private static final Logger logger = LogManager.getLogger();

    public WorkflowRpcService(
            @Nonnull WorkflowStore workflowStore,
            @Nonnull ThinTargetCache thinTargetCache) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Return a FetchWorkflowResponse with the 'workflow' field set to the corresponding Workflow.
     * If the workflow with the given id is not found, then the 'workflow' field will be empty.
     */
    @Override
    public void fetchWorkflow(WorkflowDTO.FetchWorkflowRequest workflowRequest,
                              StreamObserver<FetchWorkflowResponse> responseObserver) {
        try {
            final Optional<WorkflowDTO.Workflow> workflowResult =
                    workflowStore.fetchWorkflow(workflowRequest.getId());
            FetchWorkflowResponse.Builder responseBuilder = FetchWorkflowResponse.newBuilder();
            workflowResult
                .map(this::fillInMissingOrchestratorType)
                .ifPresent(responseBuilder::setWorkflow);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("Error while fetching the workflow with ID: " + workflowRequest.getId(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Before 8.2.2, workflows did not have an orchestrator type. As a result, we use their id to
     * determine their Type. Once the migration has been implemented, this code should be removed.
     *
     * @param workflow the workflow to check if it needs the OrchestratorType filled in.
     * @return the workflow with an OrchestratorType.
     */
    private Workflow fillInMissingOrchestratorType(Workflow workflow) {
        if(workflow.getWorkflowInfo().hasType()) {
            return workflow;
        }

        return workflow.toBuilder()
            .setWorkflowInfo(workflow.getWorkflowInfo()
                .toBuilder()
                .setType(migrateFromWorkflowName(workflow.getWorkflowInfo())))
            .build();
    }

    private OrchestratorType migrateFromWorkflowName(WorkflowInfo workflowInfo) {
        String workflowName = workflowInfo.getName();
        if (workflowName.startsWith(SDKProbeType.SERVICENOW.getProbeType())) {
            return OrchestratorType.SERVICENOW;
        }
        if (workflowName.startsWith(ACTION_STREAM_KAFKA)) {
            return OrchestratorType.ACTIONSTREAM_KAFKA;
        }
        if (workflowInfo.hasScriptPath()) {
            return OrchestratorType.ACTION_SCRIPT;
        }
        return OrchestratorType.UCSD;
    }

    @Override
    public void fetchWorkflows(FetchWorkflowsRequest request,
                               StreamObserver<FetchWorkflowsResponse> responseObserver) {
        try {
            final FetchWorkflowsResponse.Builder response = FetchWorkflowsResponse.newBuilder();
            workflowStore.fetchWorkflows(createWorkflowFilter(request)).stream()
                .map(this::fillInMissingOrchestratorType)
                .forEach(response::addWorkflows);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("Error while fetching workflows: ", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }

    @Nonnull
    private WorkflowFilter createWorkflowFilter(@Nonnull FetchWorkflowsRequest request) {
        if (request.hasOrchestratorType()) {
            throw new IllegalArgumentException("Orchestrator type filter for searching"
                    + " workflows is not implemented yet.");
        }
        return new WorkflowFilter(request.getTargetIdList());
    }

    @Override
    public void createWorkflow(
            @Nonnull final CreateWorkflowRequest request,
            @Nonnull final StreamObserver<CreateWorkflowResponse> responseObserver) {
        if (checkDiscoveredWorkflow(request.getWorkflow(), responseObserver)) {
            return;
        }

        final Optional<Long> targetIdOpt = findOnlyWebhookTarget();
        if (!targetIdOpt.isPresent()) {
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription(
                        "Could not find the webhook target needed for creating webhook workflows "
                            + "as a result we failed the request for "
                            + request.getWorkflow().getWorkflowInfo().getDisplayName()
                            + " with type " + request.getWorkflow().getWorkflowInfo().getType()
                            + " Please double check that the Webhook component is enabled.")
                    .asException());
            return;
        }
        final long targetId = targetIdOpt.get();
        createWorkflow(request, responseObserver, targetId);
    }

    private boolean checkDiscoveredWorkflow(
            final @Nonnull Workflow workflow,
            final @Nonnull StreamObserver<?> responseObserver) {
        if (!workflow.getWorkflowInfo().hasType()
                || workflow.getWorkflowInfo().getType() != OrchestratorType.WEBHOOK) {
            String errorMsg = "User defined workflows are only available for webhook as a result we failed the request for "
                + workflow.getWorkflowInfo().getDisplayName()
                + " with type " + workflow.getWorkflowInfo().getType()
                + " id: " + workflow.getId();
            logger.warn(errorMsg);
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(
                        errorMsg)
                    .asException());
            return true;
        }
        return false;
    }

    private void createWorkflow(
            final @Nonnull CreateWorkflowRequest request,
            final @Nonnull StreamObserver<CreateWorkflowResponse> responseObserver,
            final long targetId) {
        WorkflowInfo workflowInfo = request.getWorkflow().getWorkflowInfo();
        try {
            if (workflowStore.getWorkflowByDisplayName(workflowInfo.getDisplayName()).isPresent()) {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                            .withDescription(
                                "Another workflow with " + workflowInfo.getDisplayName()
                                    + " display name already exists in the system")
                            .asException());
                    return;
            }

            final WorkflowInfo workflowInfoWithTarget = workflowInfo.toBuilder()
                .setTargetId(targetId)
                .build();
            long workflowId = workflowStore.insertWorkflow(workflowInfoWithTarget);
            responseObserver.onNext(CreateWorkflowResponse.newBuilder()
                .setWorkflow(request.getWorkflow().toBuilder()
                    .setId(workflowId)
                    .setWorkflowInfo(workflowInfoWithTarget))
                .build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("Cannot create the workflow: " + workflowInfo.getDisplayName()
                    + " with type " + workflowInfo.getType(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void updateWorkflow(
            @Nonnull final UpdateWorkflowRequest request,
            @Nonnull final StreamObserver<UpdateWorkflowResponse> responseObserver) {
        Workflow currentWorkflow = request.getWorkflow();
        WorkflowInfo currentWorkflowInfo = currentWorkflow.getWorkflowInfo();
        if (checkDiscoveredWorkflow(currentWorkflow, responseObserver)) {
            return;
        }
        if (!currentWorkflow.hasId()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription(
                        "The id must be provided if you're updating the workflow."
                            + currentWorkflowInfo.getDisplayName()
                            + " with type " + currentWorkflowInfo.getType())
                    .asException());
            return;
        }

        final long workflowToUpdateId = currentWorkflow.getId();
        if (request.getId() != workflowToUpdateId) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    "The workflow id provided in the request body must be equal to the id provided as path parameter when updating the workflow."
                            + " Workflow id from request body - " + workflowToUpdateId
                            + " and from path parameters - " + request.getId() + ".")
                    .asException());
            return;
        }

        try {
            Optional<Workflow> workflow = workflowStore.getWorkflowByDisplayName(
                    currentWorkflowInfo.getDisplayName());
            if (workflow.isPresent() && workflow.get().getId() != workflowToUpdateId) {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                            .withDescription(
                                "Another workflow with " + currentWorkflowInfo.getDisplayName()
                                    + " display name already exists in the system")
                            .asException());
                    return;
            }

            Optional<Workflow> existingWorkflowOpt = workflowStore.fetchWorkflow(request.getId());
            if (!existingWorkflowOpt.isPresent()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Workflow with ID: " + request.getId() + " has not been found")
                    .asException());
                return;
            }

            Workflow existingWorkflow = existingWorkflowOpt.get();
            WorkflowInfo existingWorkflowInfo = existingWorkflow.getWorkflowInfo();

            if (immutableWorkflowParamsHaveChanged(currentWorkflowInfo, existingWorkflowInfo,
                    responseObserver, request.getId())) {
                return;
            }

            if (checkDiscoveredWorkflow(existingWorkflow, responseObserver)) {
                return;
            }

            workflowStore.updateWorkflow(
                request.getWorkflow().getId(),
                request.getWorkflow().getWorkflowInfo().toBuilder()
                    // The target id should be extracted from the existing workflow and applied to
                    // the updated workflow. That way the customer does not need to provide the
                    // target id of the hidden webhook target which can only be found through
                    // consul.
                    .setTargetId(existingWorkflowOpt.get().getWorkflowInfo().getTargetId())
                    .build());
            responseObserver.onNext(UpdateWorkflowResponse.newBuilder()
                .setWorkflow(currentWorkflow)
                .build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("Cannot update the workflow with ID: " + workflowToUpdateId, e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void deleteWorkflow(
            @Nonnull final DeleteWorkflowRequest request,
            @Nonnull final StreamObserver<DeleteWorkflowResponse> responseObserver) {
        try {
            Optional<Workflow> workflowOpt = workflowStore.fetchWorkflow(request.getId());
            if (!workflowOpt.isPresent()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Workflow with ID: " + request.getId() + " has not been found")
                    .asException());
                return;
            }
            if (checkDiscoveredWorkflow(workflowOpt.get(), responseObserver)) {
                return;
            }

            workflowStore.deleteWorkflow(request.getId());
            responseObserver.onNext(DeleteWorkflowResponse.newBuilder()
                .build());
            responseObserver.onCompleted();
        } catch (WorkflowStoreException e) {
            logger.error("Cannot delete the workflow with ID: " + request.getId(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    private Optional<Long> findOnlyWebhookTarget() {
        return thinTargetCache.getAllTargets().stream()
            .filter(targetInfo -> SDKProbeType.WEBHOOK.getProbeType().equals(targetInfo.probeInfo().type()))
            .map(ThinTargetInfo::oid)
            .findAny();
    }

    /**
     * Returns true if any of the immutable workflow parameters have changed, false otherwise.
     * The immutable parameters are: type, action type and action phase.
     *
     * @param currentWorkflow The current workflow info.
     * @param existingWorkflow The existing workflow info in the system.
     * @param responseObserver The input response observer for workflow update.
     * @param workflowId The input workflow id.
     *
     * @return true if any of the immutable workflow parameters have changed, false otherwise.
     */
    private boolean immutableWorkflowParamsHaveChanged(WorkflowInfo currentWorkflow, WorkflowInfo existingWorkflow,
            @Nonnull final StreamObserver<UpdateWorkflowResponse> responseObserver, long workflowId) {
        OrchestratorType currentWorkflowType = currentWorkflow.getType();
        OrchestratorType existingWorkflowType = existingWorkflow.getType();
        if (currentWorkflowType != existingWorkflowType) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("For workflow with ID: " + workflowId
                            + " the type can't be changed. It must match the existing one: "
                            + " current workflow type = " + currentWorkflowType.name()
                            + " existing workflow type = " + existingWorkflowType.name())
                    .asException());
            return true;
        }

        if (currentWorkflow.hasActionType()) {
            ActionType currentWorkflowActionType = currentWorkflow.getActionType();
            ActionType existingWorkflowActionType = existingWorkflow.getActionType();
            if (currentWorkflowActionType != existingWorkflowActionType) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("For workflow with ID: " + workflowId
                                + " the action type can't be changed. It must match the existing one: "
                                + " current workflow action type = " + currentWorkflowActionType.name()
                                + " existing workflow action type = " + existingWorkflowActionType.name())
                        .asException());
                return true;
            }
        }

        if (currentWorkflow.hasActionPhase()) {
            ActionPhase currentWorkflowActionPhase = currentWorkflow.getActionPhase();
            ActionPhase existingWorkflowActionPhase = existingWorkflow.getActionPhase();
            if (currentWorkflowActionPhase != existingWorkflowActionPhase) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("For workflow with ID: " + workflowId
                                + " the action phase can't be changed. It must match the existing one: "
                                + " current workflow action phase = " + currentWorkflowActionPhase.name()
                                + " existing workflow action phase = " + existingWorkflowActionPhase.name())
                        .asException());
                return true;
            }
        }

        return false;
    }

}
