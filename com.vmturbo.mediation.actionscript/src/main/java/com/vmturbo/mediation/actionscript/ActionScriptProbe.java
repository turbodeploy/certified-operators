package com.vmturbo.mediation.actionscript;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.actionscript.executor.ActionScriptExecutionStatus;
import com.vmturbo.mediation.actionscript.executor.ActionScriptExecutor;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IActionExecutor;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * A probe that will provide access to ActionScript files (any linux script file) found on
 * a linux file volume shared from the Turbonomic instance (docker host) to this mediation-actionscript
 * container. Discovery requests return a list of the script files in the 'actionsScriptPath' as
 * NonMarketEntity type Workflow. Action Execution requests will invoke the designated script file
 * within the mediation-actionscript container.
 * <p>
 * TODO: Implement a WatchService and event handlers to track ActionScript files addes or removed
 * and keep a local cache of that list. In that way the discovery request can be filled immediately
 * and not require extra file-system overhead.
 */
public class ActionScriptProbe implements IDiscoveryProbe<ActionScriptProbeAccount>,
    IActionExecutor<ActionScriptProbeAccount> {

    private IProbeContext probeContext;

    private final Logger logger = LogManager.getLogger(getClass());
    private DelegatingProgressTracker delegatingProgressTracker;

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
                           @Nullable ProbeConfiguration configuration) {

        this.probeContext = probeContext;
        final IPropertyProvider propertyProvider = this.probeContext.getPropertyProvider();
    }

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull ActionScriptProbeAccount accountValues) {
        final String targetName = accountValues.getNameOrAddress();
        logger.info("Beginning discovery of ActionScript target {}", targetName);
        try {
            final DiscoveryResponse response = new ActionScriptDiscovery(accountValues).discoverActionScripts();
            logger.info("Discovery completed for target {} with {} workflows discovered and {} errors.",
                targetName,
                response.getWorkflowCount(),
                response.getErrorDTOCount());
            return response;
        } catch (Exception e) {
            logger.error("Failed to discover target {}:", targetName, e);
            return DiscoveryResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                    .setSeverity(ErrorSeverity.CRITICAL)
                    .setDescription("ActionScript target discovery failed: "+e.getMessage())
                    .build())
                .build();
        }
    }

    @Nonnull
    @Override
    public Class<ActionScriptProbeAccount> getAccountDefinitionClass() {
        return ActionScriptProbeAccount.class;
    }

    @Nonnull
    @Override
    public List<ActionPolicyDTO> getActionPolicies() {
        /*
         * As an action orchestrator, this probe can execute almost every action, that is possible
         * in the world. As we do not have a separate feature for action orchestration, we are
         * masking the functionality under action execution. As a result, we have to provide
         * something from this method, while it is ignored on the server side.
         */
        return Collections.singletonList(
            ActionPolicyDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build());
    }

    /**
     * Validate the target. In this case we validate the path to the ActionScript folder.
     *
     * @param accountValues Account definition map.
     * @return The message of target validation status.
     */
    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull ActionScriptProbeAccount accountValues) {
        // Check to see that the filesystem folder for the ActionScripts exists and is usable.
        final String targetName = accountValues.getNameOrAddress();
        logger.info("Beginning validation of ActionScript target {} ", targetName);
        try {
            final ValidationResponse response = new ActionScriptDiscovery(accountValues).validateManifestFile();
            logger.info("Validation completed for target {} with {} errors.",
                targetName,
                response.getErrorDTOCount());
            return response;
        } catch (Exception e) {
            logger.error("Failed to validate target {}:", targetName, e);
            return ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                    .setSeverity(ErrorSeverity.CRITICAL)
                    .setDescription("ActionScript target validation failed: "+e.getMessage())
                    .build())
                .build();
        }
    }

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDto,
                                      @Nonnull final ActionScriptProbeAccount accountValues,
                                      @Nullable final Map<String, AccountValue> secondaryAccountValuesMap,
                                      @Nonnull final IProgressTracker progressTracker)
        throws InterruptedException {
        this.delegatingProgressTracker = new DelegatingProgressTracker(progressTracker);
        ActionScriptExecutor actionExecutor = new ActionScriptExecutor(accountValues, actionExecutionDto, delegatingProgressTracker);
        ActionScriptExecutionStatus status = actionExecutor.execute();
        return getActionResultForStatus(status);
    }


    @Nonnull
    private ActionResult getActionResultForStatus(ActionScriptExecutionStatus status) {
        ActionResponseState state;
        switch (status) {
            case QUEUED:
                state = ActionResponseState.QUEUED;
                break;
            case RUNNING:
                state = ActionResponseState.IN_PROGRESS;
                break;
            case COMPLETE:
                state = ActionResponseState.SUCCEEDED;
                break;
            case CANCELED:
            case CANCELED_FROM_QUEUE:
            case FAILED:
            case ERROR:
                state = ActionResponseState.FAILED;
                break;
            case NEW:
            default:
                throw new IllegalStateException("Illegal action script execution status: " + status.name());
        }
        final String description = delegatingProgressTracker.getDescription();
        return new ActionResult(state, Optional.ofNullable(description).orElse(""));
    }

    private String createDescription(String message, String addIfOutput, String output) {
        if (output != null && output.length() > 0) {
            return message + addIfOutput + "\n" + output;
        } else {
            return message;
        }
    }

    private static class DelegatingProgressTracker implements IProgressTracker {
        private IProgressTracker delegate;
        private ActionResponseState state;
        private String description;
        private int percentage;

        public DelegatingProgressTracker(final IProgressTracker delegate) {
            this.delegate = delegate;
        }

        @Override
        public void updateActionProgress(@Nonnull final ActionResponseState actionResponseState, @Nonnull final String s, final int i) {
            this.state = actionResponseState;
            this.description = s;
            this.percentage = i;
            delegate.updateActionProgress(actionResponseState, s, i);
        }

        public ActionResponseState getState() {
            return state;
        }

        public String getDescription() {
            return description;
        }

        public int getPercentage() {
            return percentage;
        }
    }

}
