package com.vmturbo.mediation.actionscript;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IActionExecutor;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.ISupplyChainAwareProbe;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * A probe that will provide access to ActionScript files (any linux script file) found on
 * a linux file volume shared from the Turbonomic instance (docker host) to this mediation-actionscript
 * container. Discovery requests return a list of the script files in the 'actionsScriptPath' as
 * NonMarketEntity type Workflow. Action Execution requests will invoke the designated script file
 * within the mediation-actionscript container.
 *
 * TODO: Implement a WatchService and event handlers to track ActionScript files addes or removed
 * and keep a local cache of that list. In that way the discovery request can be filled immediately
 * and not require extra file-system overhead.
 */
public class ActionScriptProbe implements IDiscoveryProbe<ActionScriptProbeAccount>,
    IActionExecutor<ActionScriptProbeAccount>, ISupplyChainAwareProbe<ActionScriptProbeAccount> {

    private static final Path ACTION_SCRIPTS_PATH_ROOT = Paths.get("/home/turbonomic/data/actionscript");

    private IProbeContext probeContext;

    private final Logger logger = LogManager.getLogger(getClass());
    private final ActionScriptPathValidator actionScriptPathValidator =
        new ActionScriptPathValidator();

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
                           @Nullable ProbeConfiguration configuration) {

        this.probeContext = probeContext;
        final IPropertyProvider propertyProvider = this.probeContext.getPropertyProvider();
    }

    @Override
    public void destroy() {
        // TODO: Clean up
    }

    /**
     * Discovery is not required for this target. All scripts to be executed will be
     * passed in the ActionExecutionDTO.
     * TODO: remove this method when the new IProbe support is in place
     *
     * @param accountValues Account definition map.
     * @return A set of entity DTOs for retrieved service entities.
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull ActionScriptProbeAccount accountValues) {

        logger.warn("Discovery is not supported by ActionScript mediation");
        DiscoveryResponse.Builder discoveryResponseBuilder = DiscoveryResponse.newBuilder()
            .addErrorDTO(ErrorDTO.newBuilder()
                .setSeverity(ErrorSeverity.WARNING)
                .setDescription("Discovery is not supported"));

        return discoveryResponseBuilder.build();
    }

    @Nonnull
    @Override
    public Class<ActionScriptProbeAccount> getAccountDefinitionClass() {
        return ActionScriptProbeAccount.class;
    }

    @Nonnull
    @Override
    public List<ActionPolicyDTO> getActionPolicies() {
        return Collections.emptyList();
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
        logger.info("Validate ActionScript Directory");
        return ValidationResponse.newBuilder()
            .addAllErrorDTO(actionScriptPathValidator
                .validateActionScriptPath(ACTION_SCRIPTS_PATH_ROOT)).build();
    }

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDto,
                                      @Nonnull final ActionScriptProbeAccount accountValues,
                                      @Nullable final Map<String, AccountValue> secondaryAccountValuesMap,
                                      @Nonnull final IProgressTracker progressTracker)
        throws InterruptedException {
        try (ActionScriptActionExecutor actionExecutor = new ActionScriptActionExecutor(
            probeContext)) {
            return actionExecutor.executeAction(actionExecutionDto, accountValues, progressTracker);
        }
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        return Sets.newHashSet();
    }
}
