package com.vmturbo.mediation.actionstream.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.ActionExecution.ActionErrorDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionEventDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
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
    private ActionStreamKafkaProbeProperties probeConfiguration;

    private final Logger logger = LogManager.getLogger(getClass());

    private ActionStreamKafkaProducer kafkaMessageProducer;
    private ActionStreamKafkaTopicChecker actionStreamKafkaTopicChecker;

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
                           @Nullable ProbeConfiguration configuration) {
        this.probeContext = probeContext;
        this.probeConfiguration =
                new ActionStreamKafkaProbeProperties(probeContext.getPropertyProvider());
    }

    @Override
    public void destroy() {
        if (actionStreamKafkaTopicChecker != null) {
            actionStreamKafkaTopicChecker.close();
        }
        if (kafkaMessageProducer != null) {
            kafkaMessageProducer.close();
        }
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
        final String auditTopic = accountValues.getTopic();
        boolean isTopicAvailable = getKafkaTopicChecker(accountValues).isTopicAvailable(auditTopic);
        if (isTopicAvailable) {
            return ValidationResponse.newBuilder().build();
        }
        final String errorDescription =
                "Topic " + auditTopic + " is not available on " + accountValues.getNameOrAddress()
                        + " target.";
        return ValidationResponse.newBuilder()
                .addErrorDTO(ErrorDTO.newBuilder()
                        .setDescription(errorDescription)
                        .setSeverity(ErrorSeverity.CRITICAL)
                        .build())
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
     * @return empty collection, because we send actions asynchronously
     */
    @Nonnull
    @Override
    public Collection<ActionErrorDTO> auditActions(
            @Nonnull final ActionStreamKafkaProbeAccount actionStreamKafkaProbeAccount,
            @Nonnull final Collection<ActionEventDTO> actionEvents) {
        for (ActionEventDTO actionEventDTO : actionEvents) {
            final ActionResponseState newState = actionEventDTO.getNewState();
            final ActionResponseState oldState = actionEventDTO.getOldState();
            long actionOid = actionEventDTO.getAction().getActionOid();
            logger.info("Send action {} with states transition {} -> {} for audit.", actionOid,
                    oldState, newState);
            getKafkaProducer(actionStreamKafkaProbeAccount).sendMessage(actionEventDTO,
                    actionStreamKafkaProbeAccount.getTopic());
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    ActionStreamKafkaProducer getKafkaProducer(@Nonnull ActionStreamKafkaProbeAccount accountValues) {
        if (kafkaMessageProducer == null) {
            kafkaMessageProducer = new ActionStreamKafkaProducer(
                    getKafkaProperties(getKafkaBootstrapServer(accountValues)));
        }
        return kafkaMessageProducer;
    }

    @VisibleForTesting
    ActionStreamKafkaTopicChecker getKafkaTopicChecker(
            @Nonnull ActionStreamKafkaProbeAccount accountValues) {
        if (actionStreamKafkaTopicChecker == null) {
            actionStreamKafkaTopicChecker = new ActionStreamKafkaTopicChecker(
                    getKafkaProperties(getKafkaBootstrapServer(accountValues)));
        }
        return actionStreamKafkaTopicChecker;
    }

    private Properties getKafkaProperties(@Nonnull String bootstrapServer) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", Objects.requireNonNull(bootstrapServer));
        props.put("acks", "all");
        props.put("batch.size", 16384); // in bytes
        props.put("enable.idempotence", true); // when idempotence is set to true, kafka producer will guarantee
        // that duplicate messages will not be sent, even in retry situations.
        props.put("delivery.timeout.ms", probeConfiguration.getDeliveryTimeoutMs());
        props.put("retry.backoff.ms", probeConfiguration.getRetryIntervalMs());
        props.put("max.block.ms", probeConfiguration.getMaxBlockMs());
        props.put("linger.ms", 1);
        props.put("max.request.size", probeConfiguration.getMaxRequestSizeBytes());
        props.put("buffer.memory", probeConfiguration.getMaxRequestSizeBytes());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    private String getKafkaBootstrapServer(@Nonnull final ActionStreamKafkaProbeAccount accountValues) {
        final String kafkaAddress = accountValues.getNameOrAddress();
        final String kafkaPort = accountValues.getPort();
        return kafkaAddress.concat(":").concat(kafkaPort);
    }
}
