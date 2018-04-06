package com.vmturbo.topology.processor.operation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;
import com.vmturbo.topology.processor.operation.validation.ValidationResult;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Responsible for managing all operations.
 * Any individual target may only have a single ongoing operation of a particular type
 * associated with it.
 * TODO (roman, June 16 2016): Should we allow multiple operations of different types?
 */
@ThreadSafe
public class OperationManager implements ProbeStoreListener, TargetStoreListener,
        IOperationManager {
    private final Logger logger = LogManager.getLogger();

    private final ConcurrentMap<Long, Operation> ongoingOperations = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Discovery> targetDiscoveries = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Validation> targetValidations = new ConcurrentHashMap<>();

    /**
     * A set of targets for which there are pending discoveries.
     * When a discovery completes, if a pending discovery exists for the target,
     * the pending discovery is removed and a new discovery is kicked off for the target.
     */
    private final Set<Long> pendingDiscoveries = ConcurrentHashMap.newKeySet();

    /**
     * targetId -> List of human-readable errors encountered during the last validation.
     * The list may be empty, which means the validation was successful.
     * A null entry means no validation had run.
     */
    private final ConcurrentMap<Long, ValidationResult> validationResults = new ConcurrentHashMap<>();

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final RemoteMediation remoteMediationServer;

    private final IdentityProvider identityProvider;

    private final OperationListener operationListener;

    private final EntityStore entityStore;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final long discoveryTimeoutMs;

    private final long validationTimeoutMs;

    private final long actionTimeoutMs;

    private final ExecutorService resultExecutor = Executors.newSingleThreadExecutor();

    private static final DataMetricGauge ONGOING_OPERATION_GAUGE = DataMetricGauge.builder()
        .withName("tp_ongoing_operation_total")
        .withHelp("Total number of ongoing operations in the topology processor.")
        .withLabelNames("type")
        .build()
        .register();

    private static final DataMetricSummary DISCOVERY_SIZE_SUMMARY = DataMetricSummary.builder()
        .withName("tp_discovery_size_entities")
        .withHelp("The number of service entities in a discovery.")
        .build()
        .register();

    public OperationManager(@Nonnull final IdentityProvider identityProvider,
                            @Nonnull final TargetStore targetStore,
                            @Nonnull final ProbeStore probeStore,
                            @Nonnull final RemoteMediation remoteMediationServer,
                            @Nonnull final OperationListener operationListener,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
                            @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                            final long discoveryTimeoutSeconds,
                            final long validationTimeoutSeconds,
                            final long actionTimeoutSeconds) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.remoteMediationServer = Objects.requireNonNull(remoteMediationServer);
        this.operationListener = Objects.requireNonNull(operationListener);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        this.discoveredTemplateDeploymentProfileNotifier = Objects.requireNonNull(discoveredTemplateDeploymentProfileNotifier);
        this.discoveryTimeoutMs = TimeUnit.MILLISECONDS.convert(discoveryTimeoutSeconds, TimeUnit.SECONDS);
        this.validationTimeoutMs = TimeUnit.MILLISECONDS.convert(validationTimeoutSeconds, TimeUnit.SECONDS);
        this.actionTimeoutMs = TimeUnit.MILLISECONDS.convert(actionTimeoutSeconds, TimeUnit.SECONDS);

        this.probeStore.addListener(this);
        this.targetStore.addListener(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Action requestActions(final long actionId,
                                           final long targetId,
                                           @Nonnull final List<ActionItemDTO> actionDtos)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));

        final String probeType = probeStore.getProbe(target.getProbeId())
                .map(ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                        + " corresponding to target " + targetId + " is not registered"));

        final Action action = new Action(actionId, target.getProbeId(), targetId, identityProvider);
        final ActionRequest request = ActionRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals())
                .setActionExecutionDTO(ActionExecutionDTO.newBuilder()
                        .setActionType(actionDtos.size() > 1
                        // We assume that with multiple actions, they are all MOVE/CHANGE actions
                            ? ActionType.MOVE_TOGETHER
                            : actionDtos.get(0).getActionType())
                        .addAllActionItem(actionDtos))
                .build();
        final ActionMessageHandler messageHandler = new ActionMessageHandler(this,
                action,
                remoteMediationServer.getMessageHandlerExpirationClock(),
                actionTimeoutMs);

        remoteMediationServer.sendActionRequest(target.getProbeId(),
                request, messageHandler);


        logger.info("Beginning " + action);
        operationStart(action);
        return action;
    }

    /**
     * Request a validation on a target. There may be only a single ongoing validation
     * at a time for a given target. Attempting a validation for a target with a current
     * ongoing validation will return the current ongoing validation.
     *
     * Validations may take a long time and are performed asynchronously. This method throws
     * exceptions if the validation can't initiate. If a problem
     * occurs after the validation is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * @param targetId The id of the target to validate.
     * @return The {@link Validation} requested for the given target. If there was no ongoing validation
     *         for the target with the same type, a new one will be created. If there was an ongoing validation
     *         for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Override
    @Nonnull
    public synchronized Validation startValidation(final long targetId)
            throws ProbeException, TargetNotFoundException, CommunicationException, InterruptedException {
        final Optional<Validation> currentValidation = getInProgressValidationForTarget(targetId);
        if (currentValidation.isPresent()) {
            return currentValidation.get();
        }

        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));

        final String probeType = probeStore.getProbe(target.getProbeId())
                .map(ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                        + " corresponding to target " + targetId + " is not registered"));

        final Validation validation = new Validation(target.getProbeId(),
                target.getId(), identityProvider);
        final ValidationRequest validationRequest = ValidationRequest.newBuilder()
                .setProbeType(probeType)
                .addAllAccountValue(target.getMediationAccountVals()).build();
        final ValidationMessageHandler validationMessageHandler =
                new ValidationMessageHandler(this,
                        validation,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        validationTimeoutMs);
        remoteMediationServer.sendValidationRequest(target.getProbeId(),
                validationRequest,
                validationMessageHandler);

        operationStart(validation);
        targetValidations.put(targetId, validation);

        logger.info("Beginning " + validation);
        return validation;
    }

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long)},
     * with the following exceptions:
     * 1. If a discovery is already in progress, instead of returning the existing discovery,
     *    a pending discovery will be added for the target.
     * 2. If the probe associated with the target is not currently connected, a pending discovery
     *    will be added for the target.
     *
     * When a target's discovery completes or its probe registers, the pending discovery will
     * be removed and a new discovery will be initiated for the associated target.
     *
     * @param targetId The id of the target to discover.
     * @return An {@link Optional<Discovery>}. If there was no in progress discovery
     *         for the target and the target's probe is connected, a new discovery will be initiated.
     *         If there was an in progress discovery for the target or the target's probe is disconnected,
     *         returns {@link Optional#empty()}.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Override
    @Nonnull
    public synchronized Optional<Discovery> addPendingDiscovery(long targetId)
            throws TargetNotFoundException, CommunicationException, InterruptedException {
        final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId);
        if (currentDiscovery.isPresent()) {
            // Add the target to the set of pending targets.
            pendingDiscoveries.add(targetId);
            return Optional.empty();
        }

        try {
            return Optional.of(startDiscovery(targetId));
        } catch (ProbeException e) {
            pendingDiscoveries.add(targetId);
            return Optional.empty();
        }
    }

    /**
     * Request a discovery on a target. There may be only a single ongoing discovery
     * at a time for a given target. Attempting a discovery for a target with a current
     * ongoing discovery will return the current ongoing discovery.
     *
     * Discoveries may take a long time and are performed asynchronously. This method
     * throws exceptions if the discovery can't initiate. If a problem
     * occurs after the discovery is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * @param targetId The id of the target to discover.
     * @return The {@link Discovery} requested for the given target. If there was no ongoing discovery
     *         for the target with the same type, a new one will be created. If there was an ongoing discovery
     *         for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    @Override
    public synchronized Discovery startDiscovery(final long targetId)
            throws TargetNotFoundException, ProbeException, CommunicationException, InterruptedException {
        final Optional<Discovery> currentDiscovery = getInProgressDiscoveryForTarget(targetId);
        if (currentDiscovery.isPresent()) {
            return currentDiscovery.get();
        }

        final Target target = targetStore.getTarget(targetId)
                .orElseThrow(() -> new TargetNotFoundException(targetId));

        final String probeType = probeStore.getProbe(target.getProbeId())
                .map(ProbeInfo::getProbeType)
                .orElseThrow(() -> new ProbeException("Probe " + target.getProbeId()
                        + " corresponding to target " + targetId + " is not registered"));

        final Discovery discovery = new Discovery(target.getProbeId(),
                target.getId(), identityProvider);

        final DiscoveryRequest discoveryRequest = DiscoveryRequest.newBuilder()
                .setProbeType(probeType)
                .setDiscoveryType(DiscoveryType.FULL)
                .addAllAccountValue(target.getMediationAccountVals()).build();

        final DiscoveryMessageHandler discoveryMessageHandler =
                new DiscoveryMessageHandler(this,
                        discovery,
                        remoteMediationServer.getMessageHandlerExpirationClock(),
                        discoveryTimeoutMs);

        remoteMediationServer.sendDiscoveryRequest(target.getProbeId(),
                discoveryRequest,
                discoveryMessageHandler);

        operationStart(discovery);
        targetDiscoveries.put(targetId, discovery);

        logger.info("Beginning " + discovery);
        return discovery;
    }

    @Override
    @Nonnull
    public Optional<Discovery> getInProgressDiscovery(final long id) {
        return getInProgress(id, Discovery.class);
    }

    @Override
    @Nonnull
    public Optional<Discovery> getInProgressDiscoveryForTarget(final long targetId) {
        final Discovery lastDiscovery = targetDiscoveries.get(targetId);
        return lastDiscovery == null || lastDiscovery.getStatus() != Status.IN_PROGRESS ?
                Optional.empty() : Optional.of(lastDiscovery);
    }

    @Override
    @Nonnull
    public Optional<Discovery> getLastDiscoveryForTarget(final long targetId) {
        return Optional.ofNullable(targetDiscoveries.get(targetId));
    }

    @Override
    @Nonnull
    public List<Discovery> getInProgressDiscoveries() {
        return getAllInProgress(Discovery.class);
    }

    @Override
    @Nonnull
    public Optional<Validation> getInProgressValidation(final long id) {
        return getInProgress(id, Validation.class);
    }

    @Nonnull
    public Optional<Validation> getInProgressValidationForTarget(final long targetId) {
        final Validation lastValidation = targetValidations.get(targetId);
        return lastValidation == null || lastValidation.getStatus() != Status.IN_PROGRESS ?
                Optional.empty() : Optional.of(lastValidation);
    }

    @Override
    @Nonnull
    public Optional<Validation> getLastValidationForTarget(final long targetId) {
        return Optional.ofNullable(targetValidations.get(targetId));
    }

    @Override
    @Nonnull
    public List<Validation> getAllInProgressValidations() {
        return getAllInProgress(Validation.class);
    }

    @Override
    @Nonnull
    public Optional<Action> getInProgressAction(final long id) {
        return getInProgress(id, Action.class);
    }

    @Nonnull
    public List<Action> getInProgressActions() {
        return getAllInProgress(Action.class);
    }

    /**
     * Get the latest validation result for a target.
     *
     * @param targetId The ID of the target.
     * @return A {@link ValidationResult} for the given target, or empty if
     *         no validation result exists.
     */
    @Nonnull
    public Optional<ValidationResult> getValidationResult(final long targetId) {
        return Optional.ofNullable(validationResults.get(targetId));
    }

    private void notifyOperationError(@Nonnull final Operation operation, @Nonnull ErrorDTO error) {
        if (operation instanceof Discovery) {
            final DiscoveryResponse response =
                            DiscoveryResponse.newBuilder().addErrorDTO(error).build();
            resultExecutor.execute(() -> processDiscoveryResponse((Discovery)operation, response));
        } else if (operation instanceof Validation) {
            final ValidationResponse response =
                            ValidationResponse.newBuilder().addErrorDTO(error).build();
            resultExecutor.execute(
                            () -> processValidationResponse((Validation)operation, response));
        } else if (operation instanceof Action) {
            // It's obnoxious to have to translate from ErrorDTO -> ActionResult -> ErrorDTO
            // but unfortunately not easy to avoid due to the difference in the SDK.
            final ActionResult result = ActionResult.newBuilder()
                .setResponse(ActionResponse.newBuilder()
                    .setActionResponseState(ActionResponseState.FAILED)
                    .setProgress(0)
                    .setResponseDescription(error.getDescription())
                ).build();
            resultExecutor.execute(
                () -> processActionResponse((Action)operation, result));
        }
    }

    /**
     * Notify the {@link OperationManager} that an {@link Operation} timed out.
     *
     * @param operation The {@link Operation} that timed out.
     * @param secondsSinceStart The number of seconds the operation was active for.
     */
    public void notifyTimeout(@Nonnull final Operation operation,
                              final long secondsSinceStart) {
        final ErrorDTO error = SDKUtil.createCriticalError(new StringBuilder()
                .append(operation.getClass().getSimpleName())
                .append(" ")
                .append(operation.getId())
                .append(" timed out after ")
                .append(secondsSinceStart)
                .append(" seconds.").toString());
        notifyOperationError(operation, error);
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyDiscoveryResult(@Nonnull final Discovery operation,
                             @Nonnull final DiscoveryResponse message) {
        resultExecutor.execute(() -> {
            processDiscoveryResponse(operation, message);
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyValidationResult(@Nonnull final Validation operation,
            @Nonnull final ValidationResponse message) {
        resultExecutor.execute(() -> {
            processValidationResponse(operation, message);
        });
    }

    /**
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     */
    public void notifyActionResult(@Nonnull final Action operation,
            @Nonnull final ActionResult message) {
        resultExecutor.execute(() -> {
            processActionResponse(operation, message);
        });
    }

    public void notifyProgress(@Nonnull final Operation operation,
                               @Nonnull final MediationClientMessage message) {
        if (operation instanceof Action) {
            resultExecutor.execute(() -> {
                ((Action)operation).updateProgress(message.getActionProgress().getResponse());
                operationListener.notifyOperationState(operation);
            });
        }
    }

    /**
     * Notify the {@link OperationManager} that an {@link Operation} is cancelled because transport
     * executing operation is closed.
     *
     * @param operation The {@link Operation} that timed out.
     * @param cancellationReason the reason why the operation was cancelled.
     */
    public void notifyOperationCancelled(@Nonnull final Operation operation,
                                         @Nonnull final String cancellationReason) {
        final ErrorDTO error = SDKUtil.createCriticalError(operation.getClass().getSimpleName()
                        + " " + operation.getId() + " cancelled: " + cancellationReason);
        notifyOperationError(operation, error);
    }

    /**
     * Check whether a target is associated with a pending discovery.
     * A pending discovery can be added via a call to {@link #addPendingDiscovery(long)}
     * when there is already an in progress discovery for a target. The
     *
     * @param targetId The id of the target to check for a pending discovery.
     * @return True if the target is associated with a pending discovery, false
     *         otherwise.
     */
    public boolean hasPendingDiscovery(long targetId) {
        return pendingDiscoveries.contains(targetId);
    }

    /**
     * When a probe is registered, check a discovery for any targets associated
     * with the probes that have pending discoveries.
     *
     * Executes pending target activation in a separate thread because this callback
     * occurs in the communication transport thread context and starting discoveries
     * may be an expensive operation.
     *
     * @param probeId The ID of the probe that was registered.
     * @param probe The info for the probe that was registered with the {@link ProbeStore}.
     */
    @Override
    public void onProbeRegistered(long probeId, ProbeInfo probe) {
        resultExecutor.execute(() ->
                targetStore.getProbeTargets(probeId).stream()
                    .map(Target::getId)
                    .filter(this::hasPendingDiscovery)
                    .forEach(this::activatePendingDiscovery)
        );
    }

    /**
     * When a target is removed, cancel all ongoing operations for that target.
     * Also cancel the pending discovery for the target if one exists.
     * Also upload the newly emptied group list for this target, effectively deleting any previously
     * discovered groups for that target.
     *
     * @param target The target that was removed from the {@link TargetStore}.
     */
    @Override
    public synchronized void onTargetRemoved(@Nonnull final Target target) {
        final long targetId = target.getId();
        remoteMediationServer.removeMessageHandlers(operation -> operation.getTargetId() == targetId);

        List<Operation> targetOperations = ongoingOperations.values().stream()
            .filter(operation -> operation.getTargetId() == targetId)
            .collect(Collectors.toList());

        for (Operation operation : targetOperations) {
            notifyOperationCancelled(operation, "Target removed.");
        }
        pendingDiscoveries.remove(targetId);
        discoveredGroupUploader.targetRemoved(targetId);
        discoveredTemplateDeploymentProfileNotifier.deleteTemplateDeploymentProfileByTarget(targetId);
    }

    /**
     * Check for and clear expired operations.
     */
    @Override
    public void checkForExpiredOperations() {
        remoteMediationServer.checkForExpiredHandlers();
    }

    public long getDiscoveryTimeoutMs() {
        return discoveryTimeoutMs;
    }

    public long getValidationTimeoutMs() {
        return validationTimeoutMs;
    }

    public long getActionTimeoutMs() {
        return actionTimeoutMs;
    }

    private void processValidationResponse(@Nonnull final Validation validation,
                                           @Nonnull final ValidationResponse response) {
        // Store the errors encountered during validation.
        final ValidationResult result = new ValidationResult(validation.getTargetId(), response);
        validationResults.put(validation.getTargetId(), result);

        logger.trace("Received validation result from target {}: {}", validation.getTargetId(), response);
        operationComplete(validation,
                          result.isSuccess(),
                          response.getErrorDTOList());
    }

    private void processDiscoveryResponse(@Nonnull final Discovery discovery,
                                          @Nonnull final DiscoveryResponse response) {
        final boolean success = !hasGeneralCriticalError(response.getErrorDTOList());
        final long targetId = discovery.getTargetId();
        logger.trace("Received discovery result from target {}: {}", targetId, response);

        try {
            if (success) {
                // TODO: (DavidBlinn 3/14/2018) if information makes it into the entityStore but fails later
                // the topological information will be inconsistent. (ie if the entities are placed in the
                // entityStore but the discoveredGroupUploader throws an exception, the entity and group
                // information will be inconsistent with each other because we do not roll back on failure.
                entityStore.entitiesDiscovered(discovery.getProbeId(), targetId,
                        response.getEntityDTOList());
                discoveredGroupUploader.setTargetDiscoveredGroups(targetId, response.getDiscoveredGroupList());
                discoveredTemplateDeploymentProfileNotifier.setTargetsTemplateDeploymentProfile(targetId,
                    response.getEntityProfileList(), response.getDeploymentProfileList());
                DISCOVERY_SIZE_SUMMARY.observe((double)response.getEntityDTOList().size());
            }
            operationComplete(discovery, success, response.getErrorDTOList());
        } catch (IdentityUninitializedException | IdentityMetadataMissingException |
            IdentityProviderException | RuntimeException e) {
            logger.error("Error processing discovery response: ", e);

            final ErrorDTO.Builder errorBuilder = ErrorDTO.newBuilder()
                .setSeverity(ErrorSeverity.CRITICAL);
            if (e.getLocalizedMessage() != null) {
                errorBuilder.setDescription(e.getLocalizedMessage());
            } else {
                errorBuilder.setDescription(e.getClass().getSimpleName());
            }
            operationComplete(discovery, false, Collections.singletonList(errorBuilder.build()));
        }
        activatePendingDiscovery(targetId);
    }

    private void processActionResponse(@Nonnull final Action action,
                                       @Nonnull final ActionResult result) {
        final ActionResponse response = result.getResponse();
        action.updateProgress(response);
        final boolean success = response.getActionResponseState() == ActionResponseState.SUCCEEDED;
        List<ErrorDTO> errors = success ?
            Collections.emptyList() :
            Collections.singletonList(SDKUtil.createCriticalError(response.getResponseDescription()));

        logger.trace("Received action result from target {}: {}", action.getTargetId(), result);
        operationComplete(action, success, errors);
    }

    private void activatePendingDiscovery(long targetId) {
        if (pendingDiscoveries.remove(targetId)) {
            try {
                logger.info("Activating pending discovery for " + targetId);
                startDiscovery(targetId);
            } catch (Exception e) {
                logger.error("Failed to activate discovery for "  + targetId + ": ", e);
            }
        }
    }

    private void operationStart(Operation operation) {
        ongoingOperations.put(operation.getId(), operation);
        // Send the same notification as the complete notification,
        // just that completionTime is not yet set.
        operationListener.notifyOperationState(operation);
        ONGOING_OPERATION_GAUGE.labels(operation.getClass().getName().toLowerCase()).increment();
    }

    private void operationComplete(@Nonnull final Operation operation,
                                   final boolean success,
                                   @Nonnull final List<ErrorDTO> errors) {
        operation.addErrors(errors);
        if (success) {
            operation.success();
        } else {
            operation.fail();
        }
        logger.info("Completed " + operation);

        operationListener.notifyOperationState(operation);
        ongoingOperations.remove(operation.getId());
        ONGOING_OPERATION_GAUGE.labels(operation.getClass().getName().toLowerCase()).decrement();
    }

    /**
     * Check if there was an error of critical severity that applied to the entire target.
     *
     * @param errors The list of {@link ErrorDTO} objects to check.
     * @return True if there was a target-wide critical error, false otherwise.
     */
    private boolean hasGeneralCriticalError(@Nonnull final List<ErrorDTO> errors) {
        return errors.stream()
                .anyMatch(error -> error.getSeverity() == ErrorSeverity.CRITICAL && !error.hasEntityUuid());
    }

    private <T extends Operation> Optional<T> getInProgress(final long id, Class<T> type) {
        final Operation op = ongoingOperations.get(id);
        return op != null && type.isInstance(op) ? Optional.of(type.cast(op)) : Optional.empty();
    }

    private <T extends Operation> List<T> getAllInProgress(Class<T> type) {
        final ImmutableList.Builder<T> opBuilder = new ImmutableList.Builder<>();
        ongoingOperations.values().stream()
                .filter(type::isInstance)
                .forEach(op -> opBuilder.add(type.cast(op)));
        return opBuilder.build();
    }
}
