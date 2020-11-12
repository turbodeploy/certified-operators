package com.vmturbo.mediation.actionstream.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.IActionAudit;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;

/**
 * Sends action events to an external Kafka instance. We must use an external kafka because we are
 * not permitted to expose our internal kafka instance, especially in a SaaS environment.
 */
public class ActionStreamKafkaProbe implements IDiscoveryProbe<ActionStreamKafkaProbeAccount>, IActionAudit<ActionStreamKafkaProbeAccount> {

    private static final String KAFKA_ON_GEN_WORKFLOW_ID = "ActionStreamKafka";
    private static final String KAFKA_ON_GEN_WORKFLOW_DISPLAY_NAME = "Action Stream Kafka";
    private static final String KAFKA_ON_GEN_WORKFLOW_DESCRIPTION = "Send Turbonomic actions to Kafka";

    private IProbeContext probeContext;

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
                           @Nullable ProbeConfiguration configuration) {
        this.probeContext = probeContext;
    }

    @Nonnull
    @Override
    public Class<ActionStreamKafkaProbeAccount> getAccountDefinitionClass() {
        return ActionStreamKafkaProbeAccount.class;
    }

    /**
     * Validate the target.
     *
     * @param accountValues Account definition map.
     * @return The message of target validation status.
     */
    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull ActionStreamKafkaProbeAccount accountValues) {
        // TODO(OM-64026): postponed until later so that we can unblock testing of the platform
        //                 pieces.
        return ValidationResponse.newBuilder()
            .build();
    }

    /**
     * Submit the ON_GENERATION workflow.
     *
     * @param accountValues the credentials to connect to Kafka, which are not needed for discovery.
     * @return the ON_GENERATION workflows.
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull ActionStreamKafkaProbeAccount accountValues) {
        // We need to make sure that we can still connect to kafka to ensure that the credentials
        // are still valid. Without this double check, if a customer rediscovers a target that
        // failed validation, the target will appear OK even thought validation fails.
        final ValidationResponse validationResponse = validateTarget(accountValues);

        if (validationResponse.getErrorDTOList().isEmpty()) {
            return DiscoveryResponse.newBuilder()
                .addWorkflow(Workflow.newBuilder()
                    .setId(KAFKA_ON_GEN_WORKFLOW_ID)
                    .setDisplayName(KAFKA_ON_GEN_WORKFLOW_DISPLAY_NAME)
                    .setDescription(KAFKA_ON_GEN_WORKFLOW_DESCRIPTION)
                    .setPhase(ActionScriptPhase.ON_GENERATION)
                    .build())
                // TODO(OM-64830): Add additional workflows for handling the completion of the
                //                 action below.
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
     * @param actionStreamKafkaProbeAccount the credentials to connect to Kafka,.
     * @param actionEvents the action events to send to kafka.
     * @return
     */
    @Nonnull
    @Override
    public Collection<ActionErrorDTO> auditActions(
            @Nonnull final ActionStreamKafkaProbeAccount actionStreamKafkaProbeAccount,
            @Nonnull final Collection<ActionEventDTO> actionEvents) {
        // TODO(OM-64026): postponed until later so that we can unblock testing of the platform
        //                 pieces.
        if (logger.isTraceEnabled()) {
            for (ActionEventDTO actionEvent : actionEvents) {
                logger.trace(actionEvent.toString());
            }
        }
        return Collections.emptyList();
    }
}
