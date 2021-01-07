package com.vmturbo.topology.processor.rest;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
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

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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
import com.vmturbo.topology.processor.targets.AccountValueVerifier;
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
    public static final String VALIDATING = "Validating";

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final IOperationManager operationManager;

    private final TopologyHandler topologyHandler;

    private final Scheduler scheduler;

    private final Logger logger = LogManager.getLogger();

    private final WorkflowServiceBlockingStub workflowRpcService;

    private final SettingPolicyServiceBlockingStub settingPolicyRpcService;

    private static final Collection<String> PROBES_ALLOWS_SINGLE_TARGET_INSTANCE =
            Collections.singletonList(SDKProbeType.SERVICENOW.getProbeType());

    /**
     * Constructor of {@link TargetController}.
     *
     * @param scheduler schedules events like target discovery or topology broadcast
     * @param targetStore the target store
     * @param probeStore the probe store
     * @param operationManager the operation manager to operate with probes
     * @param topologyHandler the {@link TopologyHandler} instance
     * @param settingPolicyServiceBlockingStub the setting policy service
     * @param workflowServiceBlockingStub the workflow service
     */
    public TargetController(@Nonnull final Scheduler scheduler,
            @Nonnull final TargetStore targetStore, @Nonnull final ProbeStore probeStore,
            @Nonnull IOperationManager operationManager,
            @Nonnull final TopologyHandler topologyHandler,
            @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub,
            @Nonnull final WorkflowServiceBlockingStub workflowServiceBlockingStub) {
        this.scheduler = Objects.requireNonNull(scheduler);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.settingPolicyRpcService = Objects.requireNonNull(settingPolicyServiceBlockingStub);
        this.workflowRpcService = Objects.requireNonNull(workflowServiceBlockingStub);
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
                final String probeType = probeInfo.get().getProbeType();
                if (TargetOperation.ADD.isValidTargetOperation(creationMode)) {
                    if (PROBES_ALLOWS_SINGLE_TARGET_INSTANCE.contains(probeType)) {
                        final Optional<Target> optSingleAllowedTarget = targetStore.getAll()
                                .stream()
                                .filter(target -> PROBES_ALLOWS_SINGLE_TARGET_INSTANCE.contains(
                                        target.getProbeInfo().getProbeType()))
                                .findFirst();
                        if (optSingleAllowedTarget.isPresent()) {
                            // requirement that only one instance of the target (i.g. ServiceNow)
                            // can be added in the environment
                            return errorResponse(new BadRequestException(
                                            "Cannot add more than one " + probeType
                                                    + " target. Please delete the existing '"
                                                    + optSingleAllowedTarget.get().getDisplayName()
                                                    + "' target in order to add a new target."),
                                    HttpStatus.BAD_REQUEST);
                        }
                    }
                    clearEmptyNumericAndBooleanValues(targetSpec);
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

    /**
     * Removing all the empty numeric/boolean input fields from a given target spec.
     *
     * @param targetSpec containing input fields
     */
    private void clearEmptyNumericAndBooleanValues(TargetSpec targetSpec) {
        final Set<String> numericAndBooleanFieldKeys = AccountValueVerifier.getNumericAndBooleanFieldKeys(
            probeStore.getProbe(targetSpec.getProbeId()).get()
            .getAccountDefinitionList());
        targetSpec.getInputFields().removeAll(targetSpec.getInputFields().stream()
            .filter(x -> numericAndBooleanFieldKeys.contains(x.getName())
                && x.getValue().isEmpty())
            .collect(Collectors.toSet()));
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
            final List<SettingPolicy> policiesBlockedTheDeletion =
                    getPoliciesWithWorkflowsDiscoveredByTarget(targetId);
            if (!policiesBlockedTheDeletion.isEmpty()) {
                final Collection<String> policiesBlockedDeleting =
                        Collections2.transform(policiesBlockedTheDeletion,
                                policy -> policy.getInfo().getName());
                logger.error("Failed to delete target {} because of blocking policies {}.",
                        targetId,
                        Collections2.transform(policiesBlockedTheDeletion, SettingPolicy::getId));
                return errorResponse(new ForbiddenException(
                        "Cannot remove target " + target.getDisplayName()
                                + " because there are policies related to the target: "
                                + policiesBlockedDeleting + " ."), HttpStatus.FORBIDDEN);
            }
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

    private List<SettingPolicy> getPoliciesWithWorkflowsDiscoveredByTarget(@Nonnull Long targetId) {
        final List<Workflow> workflowsDiscoveredByTarget = workflowRpcService.fetchWorkflows(
                FetchWorkflowsRequest.newBuilder().addTargetId(targetId).build())
                .getWorkflowsList();
        if (!workflowsDiscoveredByTarget.isEmpty()) {
            final List<SettingPolicy> settingPolicies = new ArrayList<>();
            settingPolicyRpcService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                    .addAllWorkflowId(
                            Collections2.transform(workflowsDiscoveredByTarget, Workflow::getId))
                    .build()).forEachRemaining(settingPolicies::add);
            return settingPolicies;
        }
        return Collections.emptyList();
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
        final Optional<Discovery> currentDiscovery =
                operationManager.getInProgressDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
        final Optional<Discovery> lastDiscovery =
                operationManager.getLastDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
        final Optional<? extends Operation> latestFinished = getLatestOperationDate(lastValidation, lastDiscovery);
        final LocalDateTime lastValidated = latestFinished.map(Operation::getCompletionTime).orElse(null);
        boolean isProbeConnected = probeStore.isProbeConnected(target.getProbeId());
        final String status = getStatus(latestFinished, currentValidation, currentDiscovery, isProbeConnected);
        return success(target, isProbeConnected, status, lastValidated);
    }

    /**
     * Returns status of the target, based on the status of the inProgressValidation
     * and discovery operations on it.
     *
     * @param latestFinished latest finished operation on the target (if present)
     * @param inProgressValidation current validationt task
     * @param inProgressDiscovery current discovery task
     * @param isProbeConnected Status of the connection to the probe.
     * @return string, representing the target status.
     */
    @Nonnull
    private String getStatus(@Nonnull Optional<? extends Operation> latestFinished,
                             @Nonnull Optional<Validation> inProgressValidation,
                             @Nonnull Optional<Discovery> inProgressDiscovery,
                             boolean isProbeConnected) {
        final String status;
        if (inProgressValidation.isPresent() && inProgressValidation.get().getUserInitiated()) {
            status = StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS;
        } else if (inProgressDiscovery.isPresent() && inProgressDiscovery.get().getUserInitiated()) {
            status = StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS;
        } else if (latestFinished.isPresent()) {
            // If there is no on-going operation which was initiated by the user - show the
            // status of the last operation.
            if (latestFinished.get().getStatus() == OperationStatus.Status.SUCCESS) {
                status = StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS;
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
     * @param discovery discovery operation
     * @return latest finished operation
     */
    @Nonnull
    private Optional<? extends Operation> getLatestOperationDate(
            @Nonnull Optional<Validation> validation, @Nonnull Optional<Discovery> discovery) {
        final Optional<? extends Operation> latestOperation = Stream.of(validation, discovery)
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
