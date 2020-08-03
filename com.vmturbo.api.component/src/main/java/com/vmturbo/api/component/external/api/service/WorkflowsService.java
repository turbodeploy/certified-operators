package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.WorkflowMapper;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.OrchestratorType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;

/**
 * Service Layer to implement the /workflows endpoints.
 **/
public class WorkflowsService implements IWorkflowsService {

    private final WorkflowServiceBlockingStub workflowServiceRpc;
    private final TargetsService targetsService;
    private final WorkflowMapper workflowMapper;

    public WorkflowsService(@Nonnull WorkflowServiceBlockingStub workflowServiceRpc,
                            @Nonnull TargetsService targetsService,
                            @Nonnull WorkflowMapper workflowMapper) {
        this.workflowServiceRpc = Objects.requireNonNull(workflowServiceRpc);
        this.targetsService = Objects.requireNonNull(targetsService);
        this.workflowMapper = Objects.requireNonNull(workflowMapper);
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
                    .valueOf(apiWorkflowType.getName()));
        }
        // fetch the workflows
        WorkflowDTO.FetchWorkflowsResponse workflowsResponse = workflowServiceRpc
                .fetchWorkflows(fetchWorkflowsBuilder.build());
        // set up a map to cache the OID -> TargetApiDTO to reduce the Target lookups
        final Map<Long, TargetApiDTO> targetMap = targetsService.getTargets(null).stream()
            .collect(Collectors.toMap(
                targetApiDTO -> Long.valueOf(targetApiDTO.getUuid()),
                Function.identity()));

        for (WorkflowDTO.Workflow workflow : workflowsResponse.getWorkflowsList()) {
            final long workflowTargetOid = workflow.getWorkflowInfo().getTargetId();
            // check the local store for the corresponding targetApiDTO
            TargetApiDTO targetApiDTO = targetMap.get(workflowTargetOid);
            if (targetApiDTO == null) {
                // Workflows with no valid target should not be included in the response
                continue;
            }
            WorkflowApiDTO dto = workflowMapper.toUiWorkflowApiDTO(workflow, targetApiDTO);
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
    public WorkflowApiDTO getWorkflowByUuid(@Nonnull String workflowUuid) throws Exception {
        Objects.requireNonNull(workflowUuid);
        // fetch the desired workflow
        WorkflowDTO.FetchWorkflowResponse fetchWorkflowResponse = workflowServiceRpc.fetchWorkflow(
                WorkflowDTO.FetchWorkflowRequest.newBuilder()
                        .setId(Long.valueOf(workflowUuid))
                        .build());
        // test to see if the workflow with that ID was found
        if (fetchWorkflowResponse.hasWorkflow()) {
            // found one
            WorkflowDTO.Workflow workflow = fetchWorkflowResponse.getWorkflow();
            // fetch the corresponding target
            String workflowTargetOid = Long.toString(workflow.getWorkflowInfo().getTargetId());
            TargetApiDTO targetApiDTO = targetsService.getTarget(workflowTargetOid);
            // map the workflow and the target to {@link WorkflowApiDTO} and return it
            return workflowMapper.toUiWorkflowApiDTO(workflow, targetApiDTO);
        } else {
            // not found
            throw new UnknownObjectException("Workflow with id: " + workflowUuid + " not found");
        }
    }
}
