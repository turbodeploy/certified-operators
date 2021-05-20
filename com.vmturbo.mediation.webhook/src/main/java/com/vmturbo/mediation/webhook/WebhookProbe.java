package com.vmturbo.mediation.webhook;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IActionAudit;
import com.vmturbo.platform.sdk.probe.IActionExecutor;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;

/**
 * Webhook probe supports sending HTTP requests to specified URL endpoint for PRE,POST, and REPLACE events.
 */
public class WebhookProbe
        implements IDiscoveryProbe<WebhookProbeAccount>, IActionAudit<WebhookProbeAccount>,
        IActionExecutor<WebhookProbeAccount> {

    private static final String WEBHOOK_ON_GEN_WORKFLOW_DESCRIPTION = "Workflow for making request to %s Webhook";

    private IProbeContext probeContext;

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
            @Nullable ProbeConfiguration configuration) {
        this.probeContext = probeContext;
    }

    @Nonnull
    @Override
    public Class<WebhookProbeAccount> getAccountDefinitionClass() {
        return WebhookProbeAccount.class;
    }

    /**
     * Validate the target.
     *
     * @param accountValues Account definition map.
     * @return The message of target validation status.
     */
    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull WebhookProbeAccount accountValues) {
        return ValidationResponse.newBuilder()
                .build();
    }

    /**
     * DiscoverTarget.
     *
     * @param accountValues the credentials to connect to Webhook, which are not needed for discovery.
     * @return the DiscoveryResponse.
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull WebhookProbeAccount accountValues) {
        // We need to make sure that we can still connect to webhook to ensure that the credentials
        // are still valid. Without this double check, if a customer rediscovers a target that
        // failed validation, the target will appear OK even thought validation fails.
        final ValidationResponse validationResponse = validateTarget(accountValues);

        if (validationResponse.getErrorDTOList().isEmpty()) {
            return DiscoveryResponse.newBuilder()
                    .addWorkflow(Workflow.newBuilder()
                            .setId(SDKUtil.hash(accountValues.getDisplayName()))
                            .setDisplayName(accountValues.getDisplayName())
                            .setDescription(String.format(WEBHOOK_ON_GEN_WORKFLOW_DESCRIPTION, accountValues.getDisplayName()))
                            .build())
                    .build();
        } else {
            final List<String> errors = validationResponse.getErrorDTOList()
                    .stream()
                    .map(ErrorDTO::getDescription)
                    .collect(Collectors.toList());
            return SDKUtil.createDiscoveryErrorAndNotification(String.format(
                    "Discovery of the target %s failed on validation step with errors: %s",
                    accountValues, String.join(", ", errors)));
        }
    }

    /**
     * Does nothing for now except logging. Full implementation will come from a future task.
     *
     * @param webhookProbeAccount the credentials to connect to Webhook,.
     * @param actionEvents the action events to send to webhook.
     * @return
     */
    @Nonnull
    @Override
    public Collection<ActionErrorDTO> auditActions(
            @Nonnull final WebhookProbeAccount webhookProbeAccount,
            @Nonnull final Collection<ActionEventDTO> actionEvents) {
        if (logger.isTraceEnabled()) {
            for (ActionEventDTO actionEvent : actionEvents) {
                logger.trace(actionEvent.toString());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public boolean supportsVersion2ActionTypes() {
        return false;
    }

    @Nonnull
    @Override
    public ActionResult executeAction(@Nonnull ActionExecutionDTO actionExecutionDTO,
            @Nonnull WebhookProbeAccount webhookProbeAccount,
            @Nullable Map<String, AccountValue> map, @Nonnull IProgressTracker iProgressTracker)
            throws InterruptedException {
        // TODO: implement
        return new ActionResult(ActionResponseState.SUCCEEDED, "");
    }

    @Nonnull
    @Override
    public List<ActionPolicyDTO> getActionPolicies() {
        // TODO: implement
        return Collections.singletonList(
                ActionPolicyDTO.newBuilder().setEntityType(EntityType.VIRTUAL_MACHINE).build());
    }
}
