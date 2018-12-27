package com.vmturbo.mediation.actionscript;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;

/**
 * Execute an ActionScript when requested. The execution will name a workflow, and the name of the
 * workflow specifies a unix command line script to be run from the base folder.
 * <p/>
 * The return code from the script execution will indicate success (0) or failure (!0).
 * <p/>
 * We will need to decide what progress messages will be returned.
 *
 * TODO: implement the script execution, status update, and 'destroy()' functionality.
 **/
public class ActionScriptActionExecutor implements AutoCloseable  {

    private final Logger logger = LogManager.getLogger();
    private final IProbeContext probeContext;

    public ActionScriptActionExecutor(final IProbeContext probeContext) {
        this.probeContext = probeContext;
    }

    @Override
    public void close() {
        // TODO: implement close for this action - may be in progress
    }

    @Nonnull
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDTO,
                                      @Nonnull final ActionScriptProbeAccount actionScriptProbeAccount,
                                      @Nonnull final IProgressTracker iProgressTracker)
        throws InterruptedException {

        if (actionExecutionDTO.hasWorkflow()) {
            final NonMarketEntityDTO actionScriptWorkflow = actionExecutionDTO.getWorkflow();
            logger.info("Executing ActionScript ", actionScriptWorkflow.getDisplayName());

            // TODO: implement the script execution with an async status update and 'wait for completion'
            // for now just indicate that the operation succeeded
            iProgressTracker.updateActionProgress(ActionResponseState.SUCCEEDED, "Done", 100);

            return new ActionResult(ActionResponseState.SUCCEEDED, "Done");
        } else {
            final String errorMessage = "ActionExecutionDTO does not have a workflow associated";
            logger.error(errorMessage + " - not done: {}");

            return new ActionResult(ActionResponseState.FAILED, errorMessage);
        }
    }
}
