package com.vmturbo.topology.processor.workflow;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;

/**
 * Transform a list of NonMarketEntityDTOs with entity type "WORKFLOW" into a
 * WorkflowInfo protobuf.
 **/
public class DiscoveredWorkflowInterpreter {

    /**
     * Scan the given list of Workflow DTOs, and create a WorkflowInfo object from each
     *
     * @param workflows a list of Workflow DTOs from a discovery response to be scanned
     * @param targetId the OID of the target from which these workflows were discovered
     * @return a list consisting of a WorkflowInfo object corresponding to each WORKFLOW DTO in the input list
     */
    @Nonnull
    public List<WorkflowInfo> interpretWorkflowList(@Nonnull List<Workflow> workflows,
                                                    long targetId) {
        return workflows.stream()
                .map(workflow -> WorkflowInfo.newBuilder()
                    .setTargetId(targetId)
                    .setName(workflow.getId())
                    .setDisplayName(workflow.getDisplayName())
                    .addAllWorkflowParam(interpretParams(workflow.getParamList()))
                    .addAllWorkflowProperty(interpretProps(workflow.getPropertyList()))
                    .build())
                .collect(Collectors.toList());
    }

    private List<WorkflowDTO.WorkflowProperty> interpretProps(List<Property> propertyList) {
        return propertyList.stream()
                .map(nmeProperty -> WorkflowDTO.WorkflowProperty.newBuilder()
                        .setName(nmeProperty.getName())
                        .setValue(nmeProperty.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    private List<WorkflowParameter> interpretParams(List<Parameter> paramList) {
        return paramList.stream()
                .map(discoveredParam -> WorkflowParameter.newBuilder()
                        .setName(discoveredParam.getName())
                        .setDescription(discoveredParam.getDescription())
                        .setType(discoveredParam.getType())
                        .setMandatory(discoveredParam.getMandatory())
                        .build())
                .collect(Collectors.toList());
    }
}
