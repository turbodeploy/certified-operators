package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.topology.UIMapping.getUserFacingCategoryString;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMMUNICATION_BINDING_CHANNEL;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;

/**
 * Mapping class that converts target info objects from topology-processor to external API DTO instances.
 */
public class TargetMapper {
    private static final Logger logger = LogManager.getLogger(TargetMapper.class);

    /**
     * The UI string constant for the "VALIDATING" state of a target.
     */
    public static final String UI_VALIDATING_STATUS = "VALIDATING";

    /**
     * The UI string constant for the "VALIDATED" state of a target.
     * Should match what's defined in the UI in stringUtils.js
     */
    private static final String UI_VALIDATED_STATUS = "Validated";

    /**
     * The display name of the category for all targets that are not functional.
     * That is usually caused by their corresponding probe not running.
     */
    private static final String UI_TARGET_CATEGORY_INOPERATIVE_TARGETS = "Inoperative Targets";

    /**
     * Map from the target statuses as defined in Topology Processor's TargetController to those
     * defined in the UI. This it pretty ugly - when we convert statuses to gRPC we should have
     * an enum instead! The UI handling of statuses is also very strange, but nothing we can do
     * there.
     */
    @VisibleForTesting
    static final Map<String, String> TARGET_STATUS_MAP =
            new ImmutableMap.Builder<String, String>()
                    .put(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS, UI_VALIDATING_STATUS)
                    .put(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS, UI_VALIDATED_STATUS)
                    .put(StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS, UI_VALIDATING_STATUS)
                    .build();

    /**
     * The default target status to use when there's no good UI mapping.
     */
    @VisibleForTesting
    static final String UNKNOWN_TARGET_STATUS = "UNKNOWN";

    private static final ZoneOffset ZONE_OFFSET = OffsetDateTime.now().getOffset();

    /**
     * Map a ProbeInfo object, returned from the Topology-Processor, into a TargetApiDTO. Note that
     * the same TargetApiDTO is used for both Probes and Targets. TODO: the InputFieldApiDTO has
     * nowhere to store the inputField.getDescription() returned from the T-P TODO: the probeInfo
     * returned from T-P has no lastValidated value TODO: the probeInfo returned has no status value
     *
     * @param probeInfo the information about this probe returned from the Topology-Processor
     * @return a {@link TargetApiDTO} mapped from the probeInfo structure given
     * @throws TargetsService.FieldVerificationException if error occurred while converting data
     */
    public TargetApiDTO mapProbeInfoToDTO(ProbeInfo probeInfo) throws TargetsService.FieldVerificationException {
        TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(Long.toString(probeInfo.getId()));
        targetApiDTO.setCategory(getUserFacingCategoryString(probeInfo.getUICategory()));
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
            throws TargetsService.FieldVerificationException {
        if (probe.getIdentifyingFields() == null || probe.getInputFields() == null) {
            return;
        }
        final Set<String> fieldNames = probe.getInputFields().stream().map(field -> field.getName())
                .collect(Collectors.toSet());
        for (String field : probe.getIdentifyingFields()) {
            if (!fieldNames.contains(field)) {
                throw new TargetsService.FieldVerificationException("Identifying field " + field
                        + " is not found among existing fields: " + fieldNames);
            }
        }
    }

    /**
     * Map the status on targetInfo to a status understood by the UI.
     *
     * <p>TP Validation-related statuses are easily mapped to UI statuses according to the
     * TARGET_STATUS_MAP. Discovery-related statuses are mapped to a UI status
     * according to the following rule:</p>
     * - If the discovery is an initial discovery (no previous validation time), tell the UI
     *   the target is "VALIDATING" because the target status is not actually known.
     * - If the discovery is NOT the initial discovery, tell the UI the target is "VALIDATED"
     *   under the assumption that the target should have been validated earlier to run
     *   subsequent discoveries. Although this is not strictly true, it at least results
     *   in reasonable behavior on target addition.
     *
     * <p>The proper fix here is to send the UI notifications about target status (and discovery
     * status) over websocket, but there is no way to do that yet.</p>
     *
     * @param targetInfo The info for the target whose status should be mapped to a value
     *                   understood by the UI.
     * @return A target status value in terms understood by the UI.
     */
    @Nonnull
    @VisibleForTesting
    public static String mapStatusToApiDTO(@Nonnull final TargetInfo targetInfo) {
        final String status = targetInfo.getStatus();
        return status == null ? UNKNOWN_TARGET_STATUS : TARGET_STATUS_MAP.getOrDefault(status, status);
    }

    /**
     * Create a {@link TargetApiDTO} instance from information in a {@link TargetInfo} object. This
     * includes the probeId, handled here, and other details based on the {@link ProbeInfo}, which
     * are added to the result {@link TargetApiDTO} by mapProbeInfoToDTO().
     *
     * @param targetInfo the {@link TargetInfo} structure returned from the Topology-Processor
     * @param probeMap a map of probeInfo indexed by probeId
     * @return a {@link TargetApiDTO} containing the target information from the given TargetInfo
     * @throws RuntimeException since you may not return a checked exception within a lambda
     *             expression
     * @throws CommunicationException if an error occurs, which will map to a 500 status error code
     */
    public TargetApiDTO mapTargetInfoToDTO(@Nonnull final TargetInfo targetInfo,
                                            @Nonnull final Map<Long, ProbeInfo> probeMap)
            throws CommunicationException {
        Objects.requireNonNull(targetInfo);
        final TargetApiDTO targetApiDTO = new TargetApiDTO();
        targetApiDTO.setUuid(Long.toString(targetInfo.getId()));
        targetApiDTO.setStatus(mapStatusToApiDTO(targetInfo));
        targetApiDTO.setReadonly(targetInfo.isReadOnly());

        if (targetInfo.getLastValidationTime() != null) {
            // UI requires Offset date time. E.g.: 2019-01-28T20:31:04.302Z
            // Assume API component is on the same timezone as topology processor (for now)
            final long epoch = targetInfo
                    .getLastValidationTime()
                    .toInstant(ZONE_OFFSET)
                    .toEpochMilli();
            targetApiDTO.setLastValidated(DateTimeUtil.toString(epoch));
        }

        // gather the other info for this target, based on the related probe
        final long probeId = targetInfo.getProbeId();
        final ProbeInfo probeInfo = probeMap.get(probeId);

        // The probeInfo object of targets should always be present because it is stored in Consul
        // to survive a topology processor restart. It is also not removed from the ProbeStore
        // when a probe disconnects.
        if (probeInfo == null) {
            // We don't expect probeInfo to be null.  Keeping this check to handling any error
            // condition if it does occur.
            targetApiDTO.setCategory(UI_TARGET_CATEGORY_INOPERATIVE_TARGETS);
            logger.error("target " + targetInfo.getId() + " - probe info not found, id: " + probeId);
            targetApiDTO.setInputFields(targetInfo.getAccountData().stream()
                    .map(this::createSimpleInputField)
                    .collect(Collectors.toList()));
        } else {
            targetApiDTO.setType(probeInfo.getType());
            targetApiDTO.setCategory(getUserFacingCategoryString(probeInfo.getUICategory()));

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
                logger.warn("AccountDefEntry not found for {} in probe with ID: {}",
                        errorFields, probeInfo.getId());
            }

            targetApiDTO.setDisplayName(targetInfo.getDisplayName());

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
            if (targetInfo.getCommunicationBindingChannel().isPresent()) {
                inputFields.add(createCommunicationChannelInputField(targetInfo.getCommunicationBindingChannel().get()));
            }
            targetApiDTO.setInputFields(inputFields);
        }

        return targetApiDTO;
    }

    @Nonnull
    private InputFieldApiDTO createCommunicationChannelInputField(@Nonnull String communicationBindingChannel) {
        final InputFieldApiDTO channelInputApiDTO = new InputFieldApiDTO();
        channelInputApiDTO.setValue(communicationBindingChannel);
        channelInputApiDTO.setName(COMMUNICATION_BINDING_CHANNEL);
        channelInputApiDTO.setIsMandatory(false);
        channelInputApiDTO.setDisplayName(COMMUNICATION_BINDING_CHANNEL);
        return channelInputApiDTO;
    }

    @Nonnull
    private InputFieldApiDTO accountDefEntryToInputField(@Nonnull final AccountDefEntry entry) {
        final InputFieldApiDTO inputFieldDTO = new InputFieldApiDTO();
        inputFieldDTO.setName(entry.getName());
        inputFieldDTO.setDisplayName(entry.getDisplayName());
        inputFieldDTO.setIsMandatory(entry.isRequired());
        inputFieldDTO.setIsSecret(entry.isSecret());
        inputFieldDTO.setIsMultiline(entry.isMultiline());
        inputFieldDTO.setValueType(convert(entry.getValueType()));
        inputFieldDTO.setDefaultValue(entry.getDefaultValue());
        inputFieldDTO.setDescription(entry.getDescription());
        inputFieldDTO.setAllowedValues(entry.getAllowedValues());
        inputFieldDTO.setVerificationRegex(entry.getVerificationRegex());
        if (entry.getDependencyField().isPresent()) {
            final Pair<String, String> dependencyKey = entry.getDependencyField().get();
            inputFieldDTO.setDependencyKey(dependencyKey.getFirst());
            inputFieldDTO.setDependencyValue(dependencyKey.getSecond());
        }

        return inputFieldDTO;
    }

    /**
     * Creates a simplified version of an InputFieldApiDTO when the {@link AccountDefEntry}
     * for the field is not available (i.e. if the probe is down).
     *
     * <p>TODO (roman, Sept 23 2016): Remove this method and it's uses once we figure
     * out the solution to keeping probe info available even when no probe of that type
     * is up.</p>
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
            case LIST:
                return InputValueType.LIST;
            default:
                throw new RuntimeException("Unrecognized account field value type: " + type);
        }
    }
}
