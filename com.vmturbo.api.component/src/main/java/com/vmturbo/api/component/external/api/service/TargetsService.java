package com.vmturbo.api.component.external.api.service;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.InputFieldApiDTO;
import com.vmturbo.api.dto.TargetApiDTO;
import com.vmturbo.api.dto.WorkflowApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;

/**
 * Service entry points to search the Repository.
 **/
public class TargetsService implements ITargetsService {

    /**
     * Indicates that validation is still in progress by the topology processor.
     */
    @VisibleForTesting
    static final String TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS = "Validation in progress";

    /**
     * Indicates that validation has successfully completed in the topology processor.
     */
    @VisibleForTesting
    static final String TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS = "Validated";

    /**
     * Indicates that validation has successfully completed in the topology processor.
     */
    @VisibleForTesting
    static final String TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS = "Discovery in progress";

    @VisibleForTesting
    static final String UI_VALIDATING_STATUS = "VALIDATING";

    /**
     * The UI string constant for the "VALIDATED" state of a target.
     * Should match what's defined in the UI in stringUtils.js
     */
    @VisibleForTesting
    static final String UI_VALIDATED_STATUS = "Validated";

    /**
     * Map from the target statuses as defined in Topology Processor's TargetController to those
     * defined in the UI. This it pretty ugly - when we convert statuses to gRPC we should have
     * an enum instead! The UI handling of statuses is also very strange, but nothing we can do
     * there.
     */
    @VisibleForTesting
    static final Map<String, String> TARGET_STATUS_MAP =
            new ImmutableMap.Builder<String, String>()
                    .put(TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS, UI_VALIDATING_STATUS)
                    .put(TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS, UI_VALIDATED_STATUS)
                    .build();

    /**
     * The default target status to use when there's no good UI mapping.
     */
    @VisibleForTesting
    static final String UNKNOWN_TARGET_STATUS = "UNKNOWN";

    static final String TARGET = "Target";

    private final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    private final Duration targetValidationTimeout;

    private final Duration targetValidationPollInterval;

    public TargetsService(@Nonnull final TopologyProcessor topologyProcessor,
                          @Nonnull final Duration targetValidationTimeout,
                          @Nonnull final Duration targetValidationPollInterval) {
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.targetValidationTimeout = Objects.requireNonNull(targetValidationTimeout);
        this.targetValidationPollInterval = Objects.requireNonNull(targetValidationPollInterval);
        logger.debug("Created TargetsService with topology processor instance {}",
                        topologyProcessor);
    }

    /**
     * Return information about the all known targets. This largely contains information from the
     * associated probe for each target. This is a blocking call that makes network requests of the
     * Topology-Processor.
     *
     * @return a List of {@link TargetApiDTO} containing the uuid of the target, plus the info from
     *         the related probe.
     * @throws Exception if there is a communication error or an error in processing in the
     *             topology-processor
     */
    @Override
    public List<TargetApiDTO> getTargets() {
        logger.debug("Get all targets");
        try {
            final Set<TargetInfo> targets = topologyProcessor.getAllTargets();
            final List<TargetApiDTO> answer = targets.stream()
                    .map(targetInfo -> {
                        try {
                            return mapTargetInfoToDTO(targetInfo);
                        } catch (CommunicationException e) {
                            throw new CommunicationError(e);
                        }
                    })
                    .collect(Collectors.toList());
            return answer;
        } catch (CommunicationException e) {
            throw new RuntimeException("Error getting targets list", e);
        }
    }

    /**
     * Return information about the given target. This largely contains information from the
     * associated probe. This is a blocking call that makes network requests of the
     * Topology-Processor.
     *
     * @param uuid the unique ID for the target
     * @return an {@link TargetApiDTO} containing the uuid of the target, plus the info from the
     *         related probe.
     * @throws Exception if there is a communication error or an error in processing in the
     *             topology-processor
     */
    @Override
    public TargetApiDTO getTarget(@PathVariable("uuid") String uuid) throws UnknownObjectException {
        logger.debug("Get target {}", uuid);
        // assumes target uuid's are long's in XL
        long targetId = Long.valueOf(uuid);
        try {
            return mapTargetInfoToDTO(topologyProcessor.getTarget(targetId));
        } catch (TopologyProcessorException e) {
            throw new UnknownObjectException(e);
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
    }

    /**
     * Return information about all the probes known to the system. This is a blocking call that
     * makes network requests of the Topology-Processor
     *
     * @return a List of information {@link TargetApiDTO} about each probe known to the system.
     * @throws Exception if there is a communication error or an error in processing in the
     *             topology-processor
     */
    @Override
    public List<TargetApiDTO> getProbes() {
        logger.debug("Get all probes");
        final Set<ProbeInfo> probes;
        try {
            probes = topologyProcessor.getAllProbes();
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
        final List<TargetApiDTO> answer = new ArrayList<>(probes.size());
        for (ProbeInfo probeInfo : probes) {
            try {
                answer.add(mapProbeInfoToDTO(probeInfo));
            } catch (FieldVerificationException e) {
                throw new RuntimeException(
                        "Fields of target " + probeInfo.getType() + " failed validation", e);
            }
        }
        return answer;
    }

    @Nonnull
    @Override
    public TargetApiDTO createTarget(@Nonnull String probeType,
            @Nonnull Collection<InputFieldApiDTO> inputFields)
            throws OperationFailedException, InterruptedException {
        logger.debug("Add target {}", probeType);
        Objects.requireNonNull(probeType);
        Objects.requireNonNull(inputFields);
        // create a target
        try {
            final Collection<ProbeInfo> probes = topologyProcessor.getAllProbes();
            final Optional<ProbeInfo> probe =
                    probes.stream().filter(pr -> probeType.equals(pr.getType())).findFirst();
            final long probeId;
            if (probe.isPresent()) {
                probeId = probe.get().getId();
            } else {
                throw new OperationFailedException("Could not find probe by type " + probeType);
            }
            final TargetData newtargetData = new NewTargetData(inputFields);
            try {
                final long targetId = topologyProcessor.addTarget(probeId, newtargetData);

                // There is an edge case where discovery may not be kicked off yet by the time add target returns.
                // Trigger validation so that topologyProcessor.getTarget is guaranteed to be able to find a status
                // for the target when it is called. This is not nice because it places (a small amount of)
                // unnecessary load on the customer infrastructure, and when the UI supports target-related
                // notifications, we should remove the call to validateTarget.
                //
                // We validate synchronously because at the time of this writing (Sept 16, 2018)
                // the UI expects the createTarget call to return after the target is validated.
                // When the UI supports target-related notifications we should remove this
                // synchronous call.
                final TargetInfo validatedTargetInfo = validateTargetSynchronously(targetId);
                return mapTargetInfoToDTO(validatedTargetInfo);
            } catch (TopologyProcessorException e) {
                throw new OperationFailedException(e);
            }
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
    }

    @Override
    public void validateAllTargets() throws InterruptedException, UnauthorizedObjectException {
        logger.debug("Validate all targets");
        // validate all targets - note that the returned status values are ignored
        try {
            topologyProcessor.validateAllTargets();
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
    }

    @Override
    public void discoverAllTargets() throws InterruptedException, UnauthorizedObjectException {
        logger.debug("Discover all targets");
        // rediscover all targets - note that the returned status values are ignored
        try {
            topologyProcessor.discoverAllTargets();
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
    }

    /**
     * Execute either a validation, a rediscovery, both, or neither, on the given target, specified
     * by uuid. This is a blocking call that makes network requests of the Topology-Processor.
     *
     * If both validation and discovery are requested, validation will be synchronously performed first. If validation
     * fails, discovery will not be performed. If validation succeeds, discovery will then be performed.
     *
     * @param uuid the uuid of the target to be operated upon - must be convertible to a Long
     * @param validate if true, then validate the target - may be null, which implies false.
     * @param rediscover if true, then rediscover the target - may be null, which implies false
     * @return the information for the target operated upon
     * @throws NumberFormatException if the uuid is not a long; CommunicationException,
     *             TopologyProcessorException if there is an error getting the target information.
     */
    @Override
    public TargetApiDTO executeOnTarget(@Nonnull String uuid, @Nullable Boolean validate,
                                        @Nullable Boolean rediscover)
            throws OperationFailedException, UnauthorizedObjectException,
            InterruptedException {
        logger.debug("Execute validate={}, discover={} on target {}", validate, rediscover, uuid);
        long targetId = Long.valueOf(uuid);

        try {
            Optional<TargetInfo> result = Optional.empty();
            if (validate != null && validate) {
                result = Optional.of(validateTargetSynchronously(targetId));
            }

            // Discover if the flag is set and, if validation was performed, it also succeeded.
            // There is no point in running a discovery when validation has just immediately failed.
            boolean shouldDiscover = rediscover != null && rediscover &&
                result.map(targetDto -> targetDto.getStatus() != null &&
                    targetDto.getStatus().equals(TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS))
                .orElse(true);

            if (shouldDiscover) {
                topologyProcessor.discoverTarget(targetId);
            }

            return mapTargetInfoToDTO(topologyProcessor.getTarget(targetId));
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        } catch (TopologyProcessorException e) {
            throw new OperationFailedException(e);
        }
    }

    /**
     * Update the definition of a target given the uuid. This operation will include two block
     * steps: udpate the target plus read the updated target back.
     *
     * @param uuid the ID of the target to update.
     * @param inputFields the field changes to make
     * @return the updated dto, read back from the targets server.
     * @throws OperationFailedException if the attempt to edit the target fails.
     * @throws UnauthorizedObjectException If the attempt to edit the target is unauthorized.
     * @throws InterruptedException if attempt to edit is interrupted.
     */
    @Nonnull
    @Override
    public TargetApiDTO editTarget(@Nonnull String uuid,
            @Nonnull Collection<InputFieldApiDTO> inputFields)
            throws OperationFailedException, UnauthorizedObjectException, InterruptedException {
        logger.debug("Edit target {}", uuid);
        long targetId = Long.valueOf(uuid);
        final TargetData updatedTargetData = new NewTargetData(inputFields);
        try {
            topologyProcessor.modifyTarget(targetId, updatedTargetData);
            return mapTargetInfoToDTO(topologyProcessor.getTarget(targetId));
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        } catch (TopologyProcessorException e) {
            throw new OperationFailedException(e);
        }
    }

    /**
     * Remove a target given the uuid. Note this is a blocking operation.
     *
     * @param uuid the ID of the target to be removed
     * @return true iff the target was removed; false otherwise.
     * @throws Exception if some extraordinary exception occurs.
     */
    @Override
    public void deleteTarget(String uuid) throws UnknownObjectException {
        logger.debug("Delete target {}", uuid);
        long targetId = Long.valueOf(uuid);
        try {
            topologyProcessor.removeTarget(targetId);
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        } catch (TopologyProcessorException e) {
            throw new UnknownObjectException(e);
        }
    }


    @Nonnull
    @Override
    public List<WorkflowApiDTO> getWorkflowsByTarget(@Nonnull String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }


    /**
     * Populate a Map from probe id to probe info for all known probes in order to facilitate lookup
     * by probe id. This is a blocking call that makes a REST API call.
     *
     * @return a Map from probeId to probeInfo for all known probes
     * @throws CommunicationException if there is a problem making the REST API call
     */
    private Map<Long, ProbeInfo> getProbeIdToProbeInfoMap() throws CommunicationException {
        // create a map of ID -> probeInfo
        return Maps.uniqueIndex(topologyProcessor.getAllProbes(), probeInfo -> probeInfo.getId());
    }

    /**
     * Define a class to hold the TargetData used to call targetsRestClient.addTarget(). There is
     * currently no convenient way to do that...but there is a JIRA item OM-10644 to remedy this.
     */
    private static class NewTargetData implements TargetData {

        private final Set<AccountValue> accountData;

        /**
         * Initialize the accountData Set from a List of InputFieldApiDTO's. We use the inputField name
         * and value to build the new AccountData value to describe the target.
         *
         * <p>Warning: The GroupProperties of the InputFieldApiDTO is ignored for now, since there is a
         * mismatch between the InputFieldApiDTO ({@code List<String>)}) and the
         * InputField.groupProperties ({@code List<List<String>>}).
         *
         * @param inputFields a list of {@ref InputFieldApiDTO} to use to initialize the accountData
         */
        NewTargetData(Collection<InputFieldApiDTO> inputFields) {
            accountData = inputFields.stream().map(inputField -> {
                return new InputField(inputField.getName(),
                                inputField.getValue(),
                                // TODO: fix type mismatch between InputFieldApiDTO.groupProperties and
                                // TargetRESTAPI.TargetSpec.InputField.groupProperties
                                // until then...return empty Optional
                                Optional.empty());
            }).collect(Collectors.toSet());
        }

        @Nonnull
        @Override
        public Set<AccountValue> getAccountData() {
            return accountData;
        }
    }

    /**
     * Map a ProbeInfo object, returned from the Topology-Processor, into a TargetApiDTO. Note that
     * the same TargetApiDTO is used for both Probes and Targets. TODO: the InputFieldApiDTO has
     * nowhere to store the inputField.getDescription() returned from the T-P TODO: the probeInfo
     * returned from T-P has no lastValidated value TODO: the probeInfo returned has no status value
     *
     * @param probeInfo the information about this probe returned from the Topology-Processor
     * @return a {@link TargetApiDTO} mapped from the probeInfo structure given
     * @throws FieldVerificationException if error occurred while converting data
     */
    private TargetApiDTO mapProbeInfoToDTO(ProbeInfo probeInfo) throws FieldVerificationException {
        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(Long.toString(probeInfo.getId()));
        targetApiDTO.setCategory(probeInfo.getCategory());
        targetApiDTO.setType(probeInfo.getType());
        targetApiDTO.setIdentifyingFields(probeInfo.getIdentifyingFields());
        List<InputFieldApiDTO> inputFields =
            probeInfo.getAccountDefinitions().stream()
                .map(this::accountDefEntryToInputField)
                .collect(Collectors.toList());
        targetApiDTO.setInputFields(inputFields);
        // TODO: targetApiDTO.setLastValidated(); is there any analog of validation in XL?
        // TODO: targetApiDTO.setStatus(); is there an analog of status in XL - in MT looks like it
        // is a text string
        verifyIdentifyingFields(targetApiDTO);
        return targetApiDTO;
    }

    private static void verifyIdentifyingFields(TargetApiDTO probe)
            throws FieldVerificationException {
        if (probe.getIdentifyingFields() == null || probe.getInputFields() == null) {
            return;
        }
        final Set<String> fieldNames = probe.getInputFields().stream().map(field -> field.getName())
                        .collect(Collectors.toSet());
        for (String field : probe.getIdentifyingFields()) {
            if (!fieldNames.contains(field)) {
                throw new FieldVerificationException("Identifying field " + field
                                + " is not found among existing fields: " + fieldNames);
            }
        }
    }

    /**
     * Map the status on targetInfo to a status understood by the UI.
     *
     * TP Validation-related statuses are easily mapped to UI statuses according to the
     * TARGET_STATUS_MAP. Discovery-related statuses are mapped to a UI status
     * according to the following rule:
     * - If the discovery is an initial discovery (no previous validation time), tell the UI
     *   the target is "VALIDATING" because the target status is not actually known.
     * - If the discovery is NOT the initial discovery, tell the UI the target is "VALIDATED"
     *   under the assumption that the target should have been validated earlier to run
     *   subsequent discoveries. Although this is not strictly true, it at least results
     *   in reasonable behavior on target addition.
     *
     * The proper fix here is to send the UI notifications about target status (and discovery
     * status) over websocket, but there is no way to do that yet.
     *
     * @param targetInfo The info for the target whose status should be mapped to a value
     *                   understood by the UI.
     * @return A target status value in terms understood by the UI.
     */
    @Nonnull
    @VisibleForTesting
    static String mapStatusToApiDTO(@Nonnull final TargetInfo targetInfo) {
        final String status = targetInfo.getStatus();
        if (status != null && status.equals(TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS)) {
            // If the target has never been validated before when a discovery occurs,
            // indicate that it's validation status is not yet known by marking it as "validating"
            return targetInfo.getLastValidationTime() == null ?
                UI_VALIDATING_STATUS : UI_VALIDATED_STATUS;
        }

        return status == null ?
            UNKNOWN_TARGET_STATUS : TARGET_STATUS_MAP.getOrDefault(status, UNKNOWN_TARGET_STATUS);
    }

    /**
     * Poll the topology processor until validation completes or the timeout for validation expires.
     * Execution of this method blocks until one of the following happens:
     * <ul>
     *     <li>Validation completes.</ul>
     *     <li>An exception is thrown.</ul>
     *     <li>The validation timeout expires.</ul>
     * </ul>
     *
     * If the timeout expires, the method returns the status of target when the timeout expires.
     *
     * @param targetId The ID of the target to be validated.
     * @return The validation status of the target upon completion or when the timeout expires.
     * @throws CommunicationException if there is an HTTP error.
     * @throws TopologyProcessorException if the update fails in the T-P.
     * @throws InterruptedException If the polling is interrupted.
     */
    @Nonnull
    @VisibleForTesting
    TargetInfo validateTargetSynchronously(final long targetId)
        throws CommunicationException, TopologyProcessorException, InterruptedException {

        topologyProcessor.validateTarget(targetId);
        Duration elapsed = Duration.ofMillis(0);
        TargetInfo targetInfo = topologyProcessor.getTarget(targetId);

        while (targetInfo.getStatus() != null &&
            targetInfo.getStatus().equals(TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS) &&
            elapsed.compareTo(targetValidationTimeout) < 0) {

            Thread.sleep(targetValidationPollInterval.toMillis());
            elapsed = elapsed.plus(targetValidationPollInterval);
            targetInfo = topologyProcessor.getTarget(targetId);
            logger.debug("Polled status of \"{}\" after waiting {}s while validating target {}",
                targetInfo.getStatus(), elapsed.getSeconds(), targetId);
        }

        return targetInfo;
    }

    /**
     * Create a {@link TargetApiDTO} instance from information in a {@link TargetInfo} object. This
     * includes the probeId, handled here, and other details based on the {@link ProbeInfo}, which
     * are added to the result {@link TargetApiDTO} by mapProbeInfoToDTO().
     *
     * @param targetInfo the {@link TargetInfo} structure returned from the Topology-Processor
     * @return a {@link TargetApiDTO} containing the target information from the given TargetInfo
     * @throws RuntimeException since you may not return a checked exception within a lambda
     *             expression
     */
    private TargetApiDTO mapTargetInfoToDTO(@Nonnull final TargetInfo targetInfo)
            throws CommunicationException {
        Objects.requireNonNull(targetInfo);
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(Long.toString(targetInfo.getId()));
        targetApiDTO.setStatus(mapStatusToApiDTO(targetInfo));

        if (targetInfo.getLastValidationTime() != null) {
            // We are relying on the UI to convert the time according
            // to the offset.
            targetApiDTO.setLastValidated(
                DateTimeFormatter.ISO_DATE_TIME.format(targetInfo.getLastValidationTime()));
        }

        // gather the other info for this target, based on the related probe
        final long probeId = targetInfo.getProbeId();
        final Map<Long, ProbeInfo> probeMap = getProbeIdToProbeInfoMap();
        final ProbeInfo probeInfo = probeMap.get(probeId);

        // As of Sept 2016, probeInfo may be null if no probe of this type
        // is currently registered. In that case we can't fill in most
        // of the information.
        if (probeInfo == null) {
            logger.warn("target " + targetInfo.getId() + " - probe info not found, id: " + probeId);
            targetApiDTO.setInputFields(targetInfo.getAccountData().stream()
                    .map(this::createSimpleInputField)
                    .collect(Collectors.toList()));
        } else {
            targetApiDTO.setType(probeInfo.getType());
            targetApiDTO.setCategory(probeInfo.getCategory());

            final Map<String, AccountValue> accountValuesByName = targetInfo.getAccountData()
                    .stream()
                    .collect(Collectors.toMap(
                        AccountValue::getName,
                        Function.identity()));

            final Set<String> probeDefinedFields = new HashSet<>();
            // There is no programmatic guarantee that account definitions won't contain duplicate
            // entries, so we add them one by one instead of using Collectors.toSet().
            probeInfo.getAccountDefinitions()
                .forEach(accountDefEntry -> probeDefinedFields.add(accountDefEntry.getName()));

            // If there are account values for fields that don't exist in the probe
            // then there's something wrong with the configuration.
            final Set<String> errorFields = Sets.difference(accountValuesByName.keySet(), probeDefinedFields);
            if (!errorFields.isEmpty()) {
                logger.error("AccountDefEntry not found for {} in probe with ID: {}",
                        errorFields, probeInfo.getId());
                throw new RuntimeException("AccountDef Entry not found for "
                        + errorFields + " in probe with id: " + probeInfo.getId());
            }

            final List<InputFieldApiDTO> inputFields = probeInfo.getAccountDefinitions().stream()
                    .map(this::accountDefEntryToInputField)
                    .map(inputFieldDTO -> {
                        final AccountValue value = accountValuesByName.get(inputFieldDTO.getName());
                        if (value != null) {
                            inputFieldDTO.setValue(value.getStringValue());
                        }
                        return inputFieldDTO;
                    })
                    .collect(Collectors.toList());
            targetApiDTO.setInputFields(inputFields);
        }

        return targetApiDTO;
    }

    @Nonnull
    private InputFieldApiDTO accountDefEntryToInputField(@Nonnull final AccountDefEntry entry) {
        final InputFieldApiDTO inputFieldDTO = new InputFieldApiDTO();
        inputFieldDTO.setName(entry.getName());
        inputFieldDTO.setDisplayName(entry.getDisplayName());
        inputFieldDTO.setIsMandatory(entry.isRequired());
        inputFieldDTO.setIsSecret(entry.isSecret());
        inputFieldDTO.setValueType(convert(entry.getValueType()));
        inputFieldDTO.setDefaultValue(entry.getDefaultValue());
        // TODO: the InputFieldApiDTO has nowhere to store the
        // inputField.getDescription()
        return inputFieldDTO;
    }

    /**
     * Creates a simplified version of an InputFieldApiDTO when the {@link AccountDefEntry}
     * for the field is not available (i.e. if the probe is down).
     *
     * TODO (roman, Sept 23 2016): Remove this method and it's uses once we figure
     * out the solution to keeping probe info available even when no probe of that type
     * is up.
     *
     * @param accountValue account value to convert
     * @return API DTO
     */
    @Nonnull
    private InputFieldApiDTO createSimpleInputField(@Nonnull final AccountValue accountValue) {
        final InputFieldApiDTO inputFieldDTO = new InputFieldApiDTO();
        inputFieldDTO.setName(accountValue.getName());
        inputFieldDTO.setValue(accountValue.getStringValue());
        // TODO: the type of these two data items don't match...cannot return
        // groupScopeProperties
        // inputFieldDTO.setGroupProperties(accountValue.getGroupScopeProperties());
        return inputFieldDTO;
    }

    /**
     * Converts TopologyProcessor's representation of account field value type into API's one.
     *
     * @param type source enum to convert from
     * @return API-specific enum
     */
    private static InputValueType convert(AccountFieldValueType type) {
        switch (type) {
            case BOOLEAN:
                return InputValueType.BOOLEAN;
            case GROUP_SCOPE:
                return InputValueType.GROUP_SCOPE;
            case NUMERIC:
                return InputValueType.NUMERIC;
            case STRING:
                return InputValueType.STRING;
            default:
                throw new RuntimeException("Unrecognized account field value type: " + type);
        }
    }

    /**
     * TargetService's dedicated exception. Used to mask communication-related exceptions as
     * runtime. So, they are automatically reported as HTTP status 500
     */
    public static class CommunicationError extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public CommunicationError(CommunicationException e) {
            super(e.getMessage(), e);
        }
    }

    /**
     * Internal exception to show, that field verification has been failed.
     */
    public static class FieldVerificationException extends Exception {
        public FieldVerificationException(String message) {
            super(message);
        }
    }
}
