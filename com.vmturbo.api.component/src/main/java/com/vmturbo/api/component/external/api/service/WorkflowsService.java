package com.vmturbo.api.component.external.api.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.api.component.external.api.mapper.WorkflowMapper;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.OrchestratorType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;

/**
 * Service Layer to implement the /workflows endpoints.
 **/
public class WorkflowsService implements IWorkflowsService {

    private final WorkflowServiceBlockingStub fetchWorkflowServiceRpc;
    private final TargetsService targetsService;

    public WorkflowsService(@Nonnull WorkflowServiceBlockingStub fetchWorkflowServiceRpc,
                            @Nonnull TargetsService targetsService) {
        this.fetchWorkflowServiceRpc = Objects.requireNonNull(fetchWorkflowServiceRpc);
        this.targetsService = Objects.requireNonNull(targetsService);
    }

    /**
     * {@inheritDoc}
     * @throws UnknownObjectException if the target for any of the returned workflows cannot be
     * found; should not happen
     * @throws IllegalArgumentException if the 'apiWorkflowType' specified is invalid.
     */
    @Override
    @Nonnull
    public List<WorkflowApiDTO> getWorkflows(@Nullable OrchestratorType apiWorkflowType)
            throws UnknownObjectException, IllegalArgumentException {
        List<WorkflowApiDTO> answer = Lists.newArrayList();
        final FetchWorkflowsRequest.Builder fetchWorkflowsBuilder = FetchWorkflowsRequest.newBuilder();
        // should we filter the workflows by Type?
        if (apiWorkflowType != null) {
            fetchWorkflowsBuilder.setOrchestratorType(WorkflowDTO.OrchestratorType
                    .valueOf(apiWorkflowType.getName()));
        }
        // fetch the workflows
        WorkflowDTO.FetchWorkflowsResponse workflowsResponse = fetchWorkflowServiceRpc
                .fetchWorkflows(fetchWorkflowsBuilder.build());
        // set up a mapping to cache the OID -> TargetApiDTO to reduce the Target lookups
        // from what we know now, there will likely be a very small number of Orchestration
        // targets - most likely only one, so this cache will be very efficient
        Map<Long, TargetApiDTO> workflowTargets = Maps.newHashMap();
        for (WorkflowDTO.Workflow workflow : workflowsResponse.getWorkflowsList()) {
            long workflowTargetOid = workflow.getWorkflowInfo().getTargetId();
            // check the local store for the corresponding targetApiDTO
            TargetApiDTO targetApiDTO = workflowTargets.get(workflowTargetOid);
            // as this call throws an exception, we cannot use 'computeIfAbsent()'
            if (targetApiDTO == null) {
                // fetch the target information
                targetApiDTO = targetsService.getTarget(Long.toString(workflowTargetOid));
                // store it for reuse
                workflowTargets.put(workflowTargetOid, targetApiDTO);
            }
            WorkflowApiDTO dto = WorkflowMapper.toUiWorkflowApiDTO(workflow, targetApiDTO);
            answer.add(dto);
        }
        return answer;
    }

    @Override
    public WorkflowApiDTO getWorkflowByUuid(String uuid) throws Exception {
        // todo: implement this call for Workflow Execution (OM-37225)
        return new WorkflowApiDTO();
    }
}
