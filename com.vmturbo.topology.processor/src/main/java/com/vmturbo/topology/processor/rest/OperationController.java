package com.vmturbo.topology.processor.rest;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.DiscoverAllResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.OperationResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.ValidateAllResponse;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Controller to handle REST requests related to operations.
 *
 * <p>The requests live under the /target namespace because all
 * operations are target-related.
 */
@Api(value = "/target")
@RequestMapping(value = "/target")
@RestController
public class OperationController {

    private final Logger logger = LogManager.getLogger();

    private final IOperationManager operationManager;

    private final Scheduler scheduler;

    private final TargetStore targetStore;

    public OperationController(@Nonnull final IOperationManager operationManager,
                               @Nonnull final Scheduler scheduler,
                               @Nonnull final TargetStore targetStore) {
        this.operationManager = Objects.requireNonNull(operationManager);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    @RequestMapping(value = "/{targetId}/discovery",
            method = RequestMethod.POST,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Discover a target given its id.",
            notes = "If discovery is already in progress for the requested target, the existing discovery is returned.")
    @ApiResponses(value = {
            @ApiResponse(code = 404,
                    message = "If the target doesn't exist in the topology processor.",
                    response = OperationResponse.class),
            @ApiResponse(code = 500,
                    message = "If failed to communicate with probe instance.",
                    response = OperationResponse.class)
    })
    public ResponseEntity<OperationResponse> discoverTarget(
            @ApiParam(value = "The target to discover.")
            @PathVariable("targetId") final Long targetId) {
        return performDiscovery(targetId);
    }


    @RequestMapping(value = "/discovery",
            method = RequestMethod.POST,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Discover all targets.",
            notes = " If discovery is not in progress for a target,"
                    + "one will attempt to be initiated. If discovery is already in progress for a target, the "
                    + "existing discovery will be returned. This request will always return a successful response "
                    + "even when some or all targets cannot be discovered.")
    public ResponseEntity<DiscoverAllResponse> discoverAllTargets() {
        return new ResponseEntity<>(
            new DiscoverAllResponse(targetStore.getAll().stream()
                .map(Target::getId)
                .map(targetId -> performDiscovery(targetId).getBody())
                .collect(Collectors.toList())
            ), HttpStatus.OK
        );
    }

    @RequestMapping(value = "/discovery/{id}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Get information about a discovery.",
            notes = "Completed discoveries are currently unsupported.")
    @ApiResponses(value = {
            @ApiResponse(code = 404,
                    message = "If the discovery doesn't exist in the topology processor.",
                    response = OperationResponse.class)
    })
    public ResponseEntity<OperationResponse> getDiscoveryById(@PathVariable("id") final Long id) {
        // TODO: Handle completed discoveries
        final OperationResponse resp = operationManager.getInProgressDiscovery(id)
                        .map(OperationController::success)
                        .orElse(OperationResponse.error("Discovery ID not found.", id));
        return new ResponseEntity<>(
                resp,
                resp.isSuccess() ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/{targetId}/discovery",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Get information about the last discovery on a target.",
            notes = "If there is a discovery in progress, returns that discovery. Otherwise, returns the last " +
                    "discovery run on the target, or an error if no discovery has ever ran on the target.")
    public ResponseEntity<OperationResponse> getTargetDiscovery(@PathVariable("targetId") final Long targetId) {
        final OperationResponse resp = operationManager.getLastDiscoveryForTarget(targetId)
                .map(OperationController::success)
                .orElse(operationManager.getInProgressDiscoveryForTarget(targetId)
                        .map(OperationController::success)
                        .orElse(OperationResponse.error("No discovery for target.", targetId)));
        return new ResponseEntity<>(resp, resp.isSuccess() ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/discovery",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "List all discoveries in progress.")
    public ResponseEntity<List<Discovery>> listOngoingDiscoveries() {
        return new ResponseEntity<>(operationManager.getInProgressDiscoveries(), HttpStatus.OK);
    }

    @RequestMapping(value = "/{targetId}/validation",
            method = RequestMethod.POST,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Validate a target given its id.",
            notes = "If validation is already in progress for the requested target, the existing validation is returned.")
    @ApiResponses(value = {
            @ApiResponse(code = 404,
                    message = "If the target doesn't exist in the topology processor.",
                    response = OperationResponse.class),
            @ApiResponse(code = 500,
                    message = "If failed to communicate with probe instance.",
                    response = OperationResponse.class)
    })
    public ResponseEntity<OperationResponse> validateTarget(
            @ApiParam(value = "The target to validate.")
            @PathVariable("targetId") final Long targetId) {
        return performValidation(targetId);
    }


    @RequestMapping(value = "/validation",
            method = RequestMethod.POST,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Validate all targets.",
            notes = " If validation is not in progress for a target,"
                    + "one will attempt to be initiated. If validation is already in progress for a target, the "
                    + "existing validation will be returned. This request will always return a successful response "
                    + "even when some or all targets cannot be validated.")
    public ResponseEntity<ValidateAllResponse> validateAllTargets() {
        return new ResponseEntity<>(
            new ValidateAllResponse(targetStore.getAll().stream()
                .map(Target::getId)
                .map(targetId -> performValidation(targetId).getBody())
                .collect(Collectors.toList())
            ), HttpStatus.OK
        );
    }

    @RequestMapping(value = "/validation/{id}",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Get information about a validation.",
            notes = "Completed validations are currently unsupported.")
    @ApiResponses(value = {
            @ApiResponse(code = 404,
                    message = "If the validation doesn't exist in the topology processor.",
                    response = OperationResponse.class)
    })
    public ResponseEntity<OperationResponse> getValidationById(@PathVariable("id") final Long id) {
        // TODO: Handle completed validation
        final OperationResponse resp = operationManager.getInProgressValidation(id)
                .map(OperationController::success)
                .orElse(OperationResponse.error("Validation ID not found.", id));
        return new ResponseEntity<>(
                resp,
                resp.isSuccess() ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/{targetId}/validation",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "Get information about the last validation on a target.",
        notes = "If there is a validation in progress, returns that validation. Otherwise, returns the last " +
                "validation run on the target, or an error if no validation has ever ran on the target.")
    public ResponseEntity<OperationResponse> getTargetValidation(@PathVariable("targetId") final Long targetId) {
        final OperationResponse resp = operationManager.getLastValidationForTarget(targetId)
                .map(OperationController::success)
                .orElse(operationManager.getInProgressValidationForTarget(targetId)
                        .map(OperationController::success)
                        .orElse(OperationResponse.error("No validation for target.", targetId)));
        return new ResponseEntity<>(resp, resp.isSuccess() ? HttpStatus.OK : HttpStatus.NOT_FOUND);
    }

    @RequestMapping(value = "/validation",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ApiOperation(value = "List all validations in progress.")
    public ResponseEntity<List<Validation>> listOngoingValidations() {
        return new ResponseEntity<>(operationManager.getAllInProgressValidations(), HttpStatus.OK);
    }

    @Nonnull
    private ResponseEntity<OperationResponse> performDiscovery(final long targetId) {
        try {
            logger.debug("PerformDiscovery for target {}", targetId);
            if (!operationManager.getInProgressDiscoveryForTarget(targetId).isPresent()) {
                scheduler.resetDiscoverySchedule(targetId);
            }
            final Discovery discovery = operationManager.startDiscovery(targetId);
            discovery.setUserInitiated(true);
            return new ResponseEntity<>(success(discovery), HttpStatus.OK);
        } catch (TargetNotFoundException e) {
            return new ResponseEntity<>(
                    OperationResponse.error("Unable to initiate discovery (" + e.getMessage() + ")", targetId),
                    HttpStatus.NOT_FOUND);
        } catch (CommunicationException | InterruptedException | ProbeException e) {
            return new ResponseEntity<>(
                    OperationResponse.error("Communication with remote probe failed: " +
                        e.getMessage(), targetId), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Nonnull
    private ResponseEntity<OperationResponse> performValidation(final long targetId) {
        try {
            final Validation validation = operationManager.startValidation(targetId);
            validation.setUserInitiated(true);
            return new ResponseEntity<>(success(validation), HttpStatus.OK);
        } catch (TargetNotFoundException e) {
            return new ResponseEntity<>(
                    OperationResponse.error("Unable to initiate validation (" + e.getMessage() + ")", targetId),
                    HttpStatus.NOT_FOUND);
        } catch (CommunicationException | InterruptedException | ProbeException e) {
            return new ResponseEntity<>(
                    OperationResponse.error("Communication with remote probe failed: " +
                        e.getMessage(), targetId), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private static OperationResponse success(Operation operation) {
        return OperationResponse.success(operation.toDto());
    }

    /**
     * Converts local date time into long type.
     *
     * @param date source date
     * @return date representation in milliseconds
     */
    @Nullable
    protected static Date toEpochMillis(@Nullable LocalDateTime date) {
        if (date == null) {
            return null;
        }
        return new Date(date.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli());
    }

}
