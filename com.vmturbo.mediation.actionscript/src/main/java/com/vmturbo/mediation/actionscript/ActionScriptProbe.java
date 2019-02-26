package com.vmturbo.mediation.actionscript;

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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
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

    private IProbeContext probeContext;

    private final Logger logger = LogManager.getLogger(getClass());
    private ActionScriptDiscovery actionScriptDiscovery = null;

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

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull ActionScriptProbeAccount accountValues) {
        final String targetName = accountValues.getNameOrAddress();
        logger.info("Beginning discovery of ActionScript target {}", targetName);
        final DiscoveryResponse response = new ActionScriptDiscovery(accountValues).discoverActionScripts();
        logger.info("Discovery completed for target {} with {} workflows discovered and {} errors.",
                targetName,
                response.getWorkflowCount(),
                response.getErrorDTOCount());
        return response;
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
        final ValidationResponse response =new ActionScriptDiscovery(accountValues).validateManifestFile();
        logger.info("Validation completed for target {} with {} errors.",
                targetName,
                response.getErrorDTOCount());
        return response;
    }

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDto,
                                      @Nonnull final ActionScriptProbeAccount accountValues,
                                      @Nullable final Map<String, AccountValue> secondaryAccountValuesMap,
                                      @Nonnull final IProgressTracker progressTracker)
        throws InterruptedException {
        try (ActionScriptActionExecutor actionExecutor = new ActionScriptActionExecutor()) {
            return actionExecutor.executeAction(actionExecutionDto, accountValues, progressTracker);
        }
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        return Sets.newHashSet();
    }

}
