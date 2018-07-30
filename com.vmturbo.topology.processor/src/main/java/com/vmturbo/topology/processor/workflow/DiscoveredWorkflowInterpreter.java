package com.vmturbo.topology.processor.workflow;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.Parameter;

/**
 * Transform a list of NonMarketEntityDTOs with entity type "WORKFLOW" into a
 * WorkflowInfo protobuf.
 **/
public class DiscoveredWorkflowInterpreter {

    /**
     * Scan the given list of NonMarketEntityDTO's, and for each where getEntityType == WORKFLOW
     * create a WorkflowInfo object from the NonMarketEntityDTO fields.
     *
     * @param nonMarketEntityDTOS a list of NonMarketEntityDTOs from a discoverey response to be
     *                            scanned looking for Workflows
     * @param targetId the OID of the target from which these workflows were discovered
     * @return a list consisting of a WorkflowInfo object corresponding to each WORKFLOW
     * NonMarketEntityDTO in the input list
     */
    @Nonnull
    public List<WorkflowInfo> interpretWorkflowList(@Nonnull List<NonMarketEntityDTO> nonMarketEntityDTOS,
                                                    long targetId) {
        return nonMarketEntityDTOS.stream()
                .filter(nonMarketEntityDTO -> NonMarketEntityDTO.NonMarketEntityType.WORKFLOW
                        .equals(nonMarketEntityDTO.getEntityType()))
                .map(nonMarketEntityDTO -> WorkflowInfo.newBuilder()
                        .setTargetId(targetId)
                        .setName(nonMarketEntityDTO.getId())
                        .setDisplayName(nonMarketEntityDTO.getDisplayName())
                        .addAllWorkflowParam(interpretParams(nonMarketEntityDTO
                                .getWorkflowData()
                                .getParamList()))
                        .build())
                .collect(Collectors.toList());
    }

    private List<WorkflowParameter> interpretParams(List<Parameter> paramList) {
        List<WorkflowParameter> answer = Lists.newArrayList();
        paramList.forEach(discoveredParam -> answer.add(WorkflowParameter.newBuilder()
                .setName(discoveredParam.getName())
                .setDescription(discoveredParam.getDescription())
                .setType(discoveredParam.getType())
                .setMandatory(discoveredParam.getMandatory())
                .build()));
        return answer;
    }

}
