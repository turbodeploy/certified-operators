package com.vmturbo.mediation.actionstream.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.action.ActionToApiConverter;
import com.vmturbo.api.conversion.action.SdkActionInformationProvider;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.enums.ActionState;
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
import com.vmturbo.platform.sdk.probe.TargetOperationException;

/**
 * Sends action events to an external Kafka instance. We must use an external kafka because we are
 * not permitted to expose our internal kafka instance, especially in a SaaS environment.
 */
public class ActionStreamKafkaProbe implements IDiscoveryProbe<ActionStreamKafkaProbeAccount>, IActionAudit<ActionStreamKafkaProbeAccount> {

    private static final ActionToApiConverter CONVERTER = new ActionToApiConverter();
    private static final String KAFKA_ON_GEN_WORKFLOW_ID = "ActionStreamKafkaOnGen";
    private static final String KAFKA_ON_GEN_WORKFLOW_DISPLAY_NAME = "Audit on action generation using Action Stream Kafka";
    private static final String KAFKA_AFTER_EXEC_WORKFLOW_ID = "ActionStreamKafkaAfterExec";
    private static final String KAFKA_AFTER_EXEC_WORKFLOW_DISPLAY_NAME = "Audit after action execution fails or completes using Action Stream Kafka";
    private static final boolean USE_API_FORMAT = true;
    private static final ObjectMapper JSON_FORMAT = new ObjectMapper();

    private ActionStreamKafkaProbeProperties probeConfiguration;

    private final Logger logger = LogManager.getLogger(getClass());

    private ActionStreamKafkaProducer kafkaMessageProducer;
    private ActionStreamKafkaTopicChecker actionStreamKafkaTopicChecker;

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
                           @Nullable ProbeConfiguration configuration) {
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

    @Override
    public boolean supportsVersion2ActionTypes() {
        return true;
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
                    .setDescription(KAFKA_ON_GEN_WORKFLOW_DISPLAY_NAME)
                    .setPhase(ActionScriptPhase.ON_GENERATION)
                    .setApiMessageFormatEnabled(USE_API_FORMAT)
                    .build())
                .addWorkflow(Workflow.newBuilder()
                    .setId(KAFKA_AFTER_EXEC_WORKFLOW_ID)
                    .setDisplayName(KAFKA_AFTER_EXEC_WORKFLOW_DISPLAY_NAME)
                    .setDescription(KAFKA_AFTER_EXEC_WORKFLOW_DISPLAY_NAME)
                    .setPhase(ActionScriptPhase.AFTER_EXECUTION)
                    .setApiMessageFormatEnabled(USE_API_FORMAT)
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
     * @param actionStreamKafkaProbeAccount the credentials to connect to Kafka,.
     * @param actionEvents the action events to send to kafka.
     * @return empty collection, because we send actions asynchronously
     */
    @Nonnull
    @Override
    public Collection<ActionErrorDTO> auditActions(
            @Nonnull final ActionStreamKafkaProbeAccount actionStreamKafkaProbeAccount,
            @Nonnull final Collection<ActionEventDTO> actionEvents) throws TargetOperationException, InterruptedException {
        final List<ActionAuditFuture> auditActionsFutures = new ArrayList<>();
        for (ActionEventDTO actionEventDTO : actionEvents) {
            final ActionResponseState newState = actionEventDTO.getNewState();
            final ActionResponseState oldState = actionEventDTO.getOldState();
            long actionOid = actionEventDTO.getAction().getActionOid();
            logger.debug("Send action {} with states transition {} -> {} for audit.", actionOid,
                    oldState, newState);
            final Future<RecordMetadata> auditActionFuture =
                    getKafkaProducer(actionStreamKafkaProbeAccount).sendMessage(getMessage(actionEventDTO),
                            actionStreamKafkaProbeAccount.getTopic());
            auditActionsFutures.add(new ActionAuditFuture(auditActionFuture, actionOid));
        }
        return waitActionsDeliveryAndCreateResponse(auditActionsFutures);
    }

    private static String getMessage(ActionEventDTO actionEventDTO) throws TargetOperationException {
        try {
            final ActionApiDTO apiMessage;
            if (actionEventDTO.getNewState() == ActionResponseState.CLEARED) {
                // By the time the action is recognized as cleared by action orchestrator, the action
                // details are no longer available. As a result, the only information we have to work
                // with is the stable actionOid and the action state. To over come this, we manually
                // construct the ActionApiDTO instead of using the api converter.
                apiMessage = new ActionApiDTO();
                long actionOid = actionEventDTO.getAction().getActionOid();
                apiMessage.setUuid(String.valueOf(actionOid));
                apiMessage.setActionID(actionOid);
                apiMessage.setActionImpactID(actionOid);
                apiMessage.setActionState(ActionState.CLEARED);
            } else {
                apiMessage = CONVERTER.convert(
                    new SdkActionInformationProvider(actionEventDTO.getAction()),
                    true,
                    0L,
                    false);
            }
            return JSON_FORMAT.writeValueAsString(apiMessage);
        } catch (JsonProcessingException e) {
            throw new TargetOperationException("Unable to translate ActionApiDTO to json", e);
        }
    }

    @Nonnull
    private List<ActionErrorDTO> waitActionsDeliveryAndCreateResponse(
            @Nonnull List<ActionAuditFuture> auditActionsFutures) throws InterruptedException {
        // Wait in order to be sure that all actions were received by external kafka. Otherwise
        // we need to send response with error and actions will be resend.
        for (ActionAuditFuture auditActionFuture : auditActionsFutures) {
            try {
                auditActionFuture.getFuture().get();
            } catch (ExecutionException e) {
                logger.error("Failed to send one of the actions from batch for audit to external "
                        + "kafka. All actions will be resend.", e);
                return Collections.singletonList(ActionErrorDTO.newBuilder()
                        .setMessage("Failed to send action to external kafka.")
                        .setActionOid(auditActionFuture.getActionOid())
                        .build());
            }
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

    /**
     * Inner class contains information about action that we send for audit and Future represents
     * the result of an asynchronous audit of the action.
     */
    @Immutable
    private class ActionAuditFuture {
        // represents the result of an asynchronous audit of the action
        private final Future<RecordMetadata> future;
        private final long actionOid;

        /**
         * Constructor of {@link ActionAuditFuture}.
         *
         * @param actionAuditFuture Future for tracking audit operation
         * @param actionOid action identifier
         */
        private ActionAuditFuture(@Nonnull Future<RecordMetadata> actionAuditFuture, long actionOid) {
            this.future = actionAuditFuture;
            this.actionOid = actionOid;
        }

        @Nonnull
        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public long getActionOid() {
            return actionOid;
        }
    }
}
