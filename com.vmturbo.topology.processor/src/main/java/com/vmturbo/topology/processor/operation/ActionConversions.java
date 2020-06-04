package com.vmturbo.topology.processor.operation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPhase;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;

public class ActionConversions {
    private static final Logger logger = LogManager.getLogger(ActionConversions.class);

    public static ActionItemDTO.ActionType convertActionType(ActionDTO.ActionType type) {
        switch (type) {
            case MOVE:
                return ActionType.MOVE;
            case SCALE:
                return ActionType.SCALE;
            case NONE:
                return ActionType.NONE;
            case RESIZE:
                // TODO (roman, May 16  2017): At the time of this writing, most probes expect RIGHT_SIZE,
                // and a few expect RESIZE. Need to remove the inconsistencies, especially if we want to
                // make this usable with third-party probes in the future.
                // Note: Above comment originally appeared in ResizeContext, but the method it appeared in was
                // removed since the interface now includes a default implementation that uses this method.
                return ActionType.RIGHT_SIZE;
            case ACTIVATE:
                return ActionType.START;
            case PROVISION:
                return ActionType.PROVISION;
            case DEACTIVATE:
                return ActionType.SUSPEND;
            case RECONFIGURE:
                return ActionType.RECONFIGURE;
            case DELETE:
                return ActionType.DELETE;
            default:
                logger.warn("Unrecognized action type: {}", type.name());
                return null;
        }
    }

    public static ActionScriptPhase convertActionPhase(ActionPhase phase) {
        switch (phase) {
            case ON_GENERATION:
                return ActionScriptPhase.ON_GENERATION;
            case APPROVAL:
                return ActionScriptPhase.APPROVAL;
            case PRE:
                return ActionScriptPhase.PRE;
            case REPLACE:
                return ActionScriptPhase.REPLACE;
            case POST:
                return ActionScriptPhase.POST;
            case AFTER_EXECUTION:
                return ActionScriptPhase.AFTER_EXECUTION;
            default:
                logger.warn("Unknown ActionPhase: " + phase.name());
                return null;
        }
    }
}
