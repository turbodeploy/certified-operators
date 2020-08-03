package com.vmturbo.topology.processor.workflow;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo.Builder;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Property;

/**
 * Transform a list of NonMarketEntityDTOs with entity type "WORKFLOW" into a
 * WorkflowInfo protobuf.
 **/
public class DiscoveredWorkflowInterpreter {

    private static final Logger logger = LoggerFactory.getLogger(DiscoveredWorkflowInterpreter.class);

    /**
     * Scan the given list of Workflow DTOs, and create a WorkflowInfo object from each
     *
     * @param workflows a list of {@link Workflow} DTOs from a discovery response to be scanned
     * @param targetId the OID of the target from which these workflows were discovered
     * @return a list consisting of a WorkflowInfo object corresponding to each WORKFLOW DTO in the input list
     */
    @Nonnull
    public List<WorkflowInfo> interpretWorkflowList(@Nonnull List<Workflow> workflows,
                                                    long targetId) {
        return workflows.stream()
            .map(workflow -> {
                final Builder wfBuilder = WorkflowInfo.newBuilder()
                    .setTargetId(targetId);
                if (workflow.hasId()) {
                    wfBuilder.setName(workflow.getId());
                }
                if (workflow.hasDisplayName()) {
                    wfBuilder.setDisplayName(workflow.getDisplayName());
                }
                if (workflow.hasDescription()) {
                    wfBuilder.setDescription(workflow.getDescription());
                }
                wfBuilder.addAllWorkflowParam(interpretParams(workflow.getParamList()));
                wfBuilder.addAllWorkflowProperty(interpretProps(workflow.getPropertyList()));
                if (workflow.hasScriptPath()) {
                    wfBuilder.setScriptPath(workflow.getScriptPath());
                }
                if (workflow.hasEntityType()) {
                    wfBuilder.setEntityType(workflow.getEntityType().getNumber());
                }
                if (workflow.hasActionType()) {
                    ActionType converted = convertActionType(workflow.getActionType());
                    if (converted != null) {
                        wfBuilder.setActionType(converted);
                    }
                }
                if (workflow.hasPhase()) {
                    final ActionPhase converted = convertActionPhase(workflow.getPhase());
                    if (converted != null) {
                        wfBuilder.setActionPhase(converted);
                    }
                }
                if (workflow.hasTimeLimitSeconds()) {
                    wfBuilder.setTimeLimitSeconds(workflow.getTimeLimitSeconds());
                }
                return wfBuilder.build();
            }).collect(Collectors.toList());
    }

    private ActionDTO.ActionType convertActionType(ActionItemDTO.ActionType type) {
        switch (type) {
            case MOVE:
                return ActionType.MOVE;
            case SCALE:
                return ActionType.SCALE;
            case NONE:
                return ActionType.NONE;
            case PROVISION:
                return ActionType.PROVISION;
            case SUSPEND:
                return ActionType.DEACTIVATE;
            case RESIZE:
                return ActionType.RESIZE;
            case START:
                return ActionType.ACTIVATE;
            case RECONFIGURE:
                return ActionType.RECONFIGURE;
            default:
                logger.warn("Unrecognized action type: {}", type);
                return null;
        }
    }

    private ActionDTO.ActionPhase convertActionPhase(Workflow.ActionScriptPhase phase) {
        switch(phase) {
            case PRE:
                return ActionPhase.PRE;
            case REPLACE:
                return ActionPhase.REPLACE;
            case POST:
                return ActionPhase.POST;
            case ON_GENERATION:
                return ActionPhase.ON_GENERATION;
            case APPROVAL:
                return ActionPhase.APPROVAL;
            case AFTER_EXECUTION:
                return ActionPhase.AFTER_EXECUTION;
            default:
                logger.warn("Unrecognized action phase: {}", phase);
                return null;
        }
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
