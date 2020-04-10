package com.vmturbo.topology.processor.rest;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.ForbiddenException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.GetAllTargetsResponse;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;


/**
 * Controller for the REST interface for target management.
 * Uses Spring annotations.
 */
@Api(value = "/target")
@RequestMapping(value = "/target")
@RestController
public class TargetController {

    @VisibleForTesting
    public static final String VALIDATED = "Validated";

    @VisibleForTesting
    public static final String VALIDATING = "Validating";

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final IOperationManager operationManager;

    private final TopologyHandler topologyHandler;

    private final Scheduler scheduler;

    private final Logger logger = LogManager.getLogger();

    public TargetController(@Nonnull final Scheduler scheduler, @Nonnull final TargetStore targetStore,
                            @Nonnull final ProbeStore probeStore, @Nonnull IOperationManager operationManager,
                            @Nonnull final TopologyHandler topologyHandler) {
        this.scheduler = Objects.requireNonNull(scheduler);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
    }

    @RequestMapping(method = RequestMethod.POST,
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Add a target.")
    @ApiResponses(value = {
            @ApiResponse(code = 400,
               message = "If there are errors validating the target info, for example if required properties are missing.",
               response = TargetInfo.class)
    })
    public ResponseEntity<TargetInfo> addTarget(
            @ApiParam(value = "The information for the target to add.", required = true)
            @RequestBody final TargetSpec targetSpec) {
        try {
            final Optional<ProbeInfo> probeInfo = probeStore.getProbe(targetSpec.toDto().getProbeId());
            if (probeInfo.isPresent()) {
                final CreationMode creationMode = probeInfo.get().getCreationMode();
                if (TargetOperation.ADD.isValidTargetOperation(creationMode)) {
                    TopologyProcessorDTO.TargetSpec targetDto
                            = probeInfo.get().getCreationMode() == CreationMode.INTERNAL
                            ? targetSpec.toDto().toBuilder().setIsHidden(true).build()
                            : targetSpec.toDto();
                    final Target target = targetStore.createTarget(targetDto);
                    final TargetInfo targetInfo = targetToTargetInfo(target);
                    return new ResponseEntity<>(targetInfo, HttpStatus.OK);
                } else {
                    // invalid operation
                    return errorResponse(new ForbiddenException("ADD operation is not allowed on "
                        + targetSpec.getProbeId()), HttpStatus.FORBIDDEN);
                }
            } else {
                return errorResponse(new ForbiddenException("Target probe was not found: "
                    + targetSpec.getProbeId()), HttpStatus.NOT_FOUND);
            }
        } catch (TopologyProcessorException | IdentityStoreException | DuplicateTargetException e) {
            return errorResponse(e, HttpStatus.BAD_REQUEST);
        } catch (InvalidTargetException e) {
            final TargetInfo resp = error(e.getErrors());
            return new ResponseEntity<>(resp, HttpStatus.BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/{targetId}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Get target information by id.",
                  notes = "Secret accound fields are absent in the response.")
    @ApiResponses(value = {
            @ApiResponse(code = 404,
                message = "If the target doesn't exist in the topology processor.",
                response = TargetInfo.class)
    })
    public ResponseEntity<TargetInfo> getTarget(
            @ApiParam(value = "The ID of the target.")
            @PathVariable("targetId") final Long targetId) {
        final TargetInfo resp = targetStore.getTarget(targetId)
                .map(this::targetToTargetInfo)
                .orElse(error(targetId, "Target not found."));
        return new ResponseEntity<>(resp,
                resp.getErrors() == null ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

    @RequestMapping(method = RequestMethod.GET,
                    produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @ApiOperation(value = "Get all registered targets.")
    public ResponseEntity<GetAllTargetsResponse> getAllTargets() {
        final List<TargetInfo> allTargets = targetStore.getAll().stream ()
                        .map(this::targetToTargetInfo)
                        .collect(Collectors.toList());
        final GetAllTargetsResponse resp = new GetAllTargetsResponse(allTargets);
        return new ResponseEntity<>(resp, HttpStatus.OK);
    }

    @RequestMapping(value = "/{targetId}", method = RequestMethod.PUT,
                    produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @ApiOperation(value = "Update existing target.")
    @ApiResponses(value = {
                    @ApiResponse(code = 400,
                                    message = "If there are errors validating the target info, for example if required properties are missing.",
                                    response = TargetInfo.class),
                    @ApiResponse(code = 404,
                                    message = "If target requested for update does not exist.",
                                    response = TargetInfo.class)

    })
    public ResponseEntity<TargetInfo> updateTarget(
                    @ApiParam(value = "The information for the target to update.",
                                    required = true) @RequestBody final Collection<InputField> targetSpec,
                    @ApiParam(value = "The ID of the target.") @PathVariable("targetId") final Long targetId) {
        try {
            Target target = getTargetFromStore(targetId);
            if (TargetOperation.UPDATE.isValidTargetOperation(target.getProbeInfo().getCreationMode())) {
                Objects.requireNonNull(targetSpec);
                target = targetStore.updateTarget(targetId, targetSpec.stream()
                    .map(av -> av.toAccountValue()).collect(Collectors.toList()));
                final TargetInfo targetInfo = targetToTargetInfo(target);
                return new ResponseEntity<>(targetInfo, HttpStatus.OK);
            } else {
                // invalid operation
                return errorResponse(new ForbiddenException("UPDATE operation is not allowed on "
                    + target.getDisplayName()), HttpStatus.FORBIDDEN);
            }
        } catch (InvalidTargetException e) {
            final TargetInfo resp = error(e.getErrors());
            return new ResponseEntity<>(resp, HttpStatus.BAD_REQUEST);
        } catch (IdentifierConflictException e) {
            return errorResponse(e, HttpStatus.BAD_REQUEST);
        } catch (IdentityStoreException e) {
            return errorResponse(e, HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (TargetNotFoundException e) {
            return errorResponse(e, HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(value = "/{targetId}", method = RequestMethod.DELETE,
                    produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @ApiOperation(value = "Remove existing target and trigger a broadcast.")
    @ApiResponses(value = {
                    @ApiResponse(code = 404,
                                    message = "If target requested for update does not exist.",
                                    response = TargetInfo.class)

    })
    public ResponseEntity<TargetInfo> removeTarget(@ApiParam(
                    value = "The ID of the target.") @PathVariable("targetId") final Long targetId) {
        try {
            Target target = getTargetFromStore(targetId);
            if (TargetOperation.REMOVE.isValidTargetOperation(target.getProbeInfo().getCreationMode())) {
                target = targetStore.removeTargetAndBroadcastTopology(targetId, topologyHandler, scheduler);
                return new ResponseEntity<>(targetToTargetInfo(target), HttpStatus.OK);
            } else {
                // invalid operation
                return errorResponse(new ForbiddenException("Operation remove is not allowed on "
                    + target.getDisplayName()), HttpStatus.FORBIDDEN);
            }
        } catch (TargetNotFoundException e) {
            return errorResponse(e, HttpStatus.NOT_FOUND);
        } catch (IdentityStoreException e) {
            return errorResponse(e, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Nonnull
    private Target getTargetFromStore(@Nonnull final Long targetId) throws TargetNotFoundException {
        return targetStore.getTarget(targetId).orElseThrow(() -> new TargetNotFoundException(targetId)); }

    private ResponseEntity<TargetInfo> errorResponse(Throwable exception, HttpStatus status) {
        final TargetInfo targetInfo = error(null, exception.getMessage());
        return new ResponseEntity<>(targetInfo, status);
    }

    private TargetInfo targetToTargetInfo(@Nonnull final Target target) {
        final Optional<Validation> currentValidation =
                operationManager.getInProgressValidationForTarget(target.getId());
        final Optional<Validation> lastValidation =
                operationManager.getLastValidationForTarget(target.getId());
        final Optional<Discovery> currentFullDiscovery =
                operationManager.getInProgressDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
        final Optional<Discovery> lastDiscovery =
                operationManager.getLastDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
        final Optional<Discovery> currentIncrementalDiscovery =
            operationManager.getInProgressDiscoveryForTarget(target.getId(), DiscoveryType.INCREMENTAL);
        final Optional<Discovery> lastIncrementalDiscovery =
            operationManager.getLastDiscoveryForTarget(target.getId(), DiscoveryType.INCREMENTAL);
        // todo: vc always return empty response for incremental discovery if network connection is
        // not available, which acts like a successful discovery, this is wrong. we should add
        // lastIncrementalDiscovery to getLatestOperationDate once it's fixed
        final Optional<? extends Operation> latestFinished = getLatestOperationDate(lastValidation, lastDiscovery);
        final LocalDateTime lastValidated = latestFinished.map(Operation::getCompletionTime).orElse(null);
        boolean isProbeConnected = probeStore.isProbeConnected(target.getProbeId());
        final String status = getStatus(latestFinished, currentValidation, currentFullDiscovery,
            currentIncrementalDiscovery, isProbeConnected);
        return success(target, isProbeConnected, status, lastValidated);
    }

    /**
     * Returns status of the target, based on the status of the inProgressValidation
     * and discovery operations on it.
     *
     * @param latestFinished latest finished operation on the target (if present)
     * @param inProgressValidation current validationt task
     * @param inProgressFullDiscovery current full discovery task
     * @param inProgressIncrementalDiscovery current incremental discovery task
     * @param isProbeConnected Status of the connection to the probe.
     * @return string, representing the target status.
     */
    @Nonnull
    private String getStatus(@Nonnull Optional<? extends Operation> latestFinished,
                             @Nonnull Optional<Validation> inProgressValidation,
                             @Nonnull Optional<Discovery> inProgressFullDiscovery,
                             @Nonnull Optional<Discovery> inProgressIncrementalDiscovery,
                             boolean isProbeConnected) {
        final String status;
        if (inProgressValidation.isPresent() && inProgressValidation.get().getUserInitiated()) {
            status = "Validation in progress";
        } else if (inProgressFullDiscovery.isPresent() && inProgressFullDiscovery.get().getUserInitiated()) {
            status = "Full discovery in progress";
        } else if (inProgressIncrementalDiscovery.isPresent() && inProgressIncrementalDiscovery.get().getUserInitiated()) {
            status = "Incremental discovery in progress";
        } else if (latestFinished.isPresent()) {
            // If there is no on-going operation which was initiated by the user - show the
            // status of the last operation.
            if (latestFinished.get().getStatus() == OperationStatus.Status.SUCCESS) {
                status = VALIDATED;
            } else {
                status = latestFinished.get().getErrorString();
            }
        } else if (!isProbeConnected) {
            status = "Failed to connect to probe. Check if probe is running";
        } else {
            // If the target status is unknown, show as "Validating"
            status = VALIDATING;
        }
        return status;
    }

    /**
     * Returns the latest finished operation, chosen from the ones specified.
     *
     * @param validation validation operation
     * @param fullDiscovery full discovery operation
     * @return latest finished operation
     */
    @Nonnull
    private Optional<? extends Operation> getLatestOperationDate(
            @Nonnull Optional<Validation> validation, @Nonnull Optional<Discovery> fullDiscovery) {
        final Optional<? extends Operation> latestOperation =
            Stream.of(validation, fullDiscovery)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(op -> op.getCompletionTime() != null)
                .max(Comparator.comparing(Operation::getCompletionTime));
        return latestOperation;
    }

    private static TargetInfo error(final Long targetId, @Nonnull final String err) {
        String error = Objects.requireNonNull(err);
        return new TargetInfo(targetId, null, ImmutableList.of(error), null, null, null, null);
    }

    private static TargetInfo error(@Nonnull final List<String> errors) {
        return new TargetInfo(null, null, errors, null, null, null, null);
    }

    public static TargetInfo success(@Nonnull final Target target, final boolean probeConnected,
            @Nonnull final String targetStatus, @Nullable final LocalDateTime lastValidation) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(targetStatus);
        return new TargetInfo(target.getId(), target.getDisplayName(), null,
                new TargetSpec(target.getNoSecretDto().getSpec()), probeConnected,
                targetStatus, lastValidation);
    }

}
