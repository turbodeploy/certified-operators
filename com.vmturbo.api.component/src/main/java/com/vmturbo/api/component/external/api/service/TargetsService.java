package com.vmturbo.api.component.external.api.service;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;

import com.vmturbo.api.TargetNotificationDTO.TargetNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification.TargetStatus;
import com.vmturbo.api.component.communication.ApiComponentTargetListener;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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

    @VisibleForTesting
    static final String UI_VALIDATING_STATUS = "VALIDATING";

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

    /**
     * This is currently required because the SDK probes have
     * the category field inconsistently cased
     */
    private static final ImmutableBiMap<String, String> USER_FACING_CATEGORY_MAP = ImmutableBiMap
            .<String, String>builder()
            .put("STORAGE", "Storage")
            .put("HYPERVISOR", "Hypervisor")
            .put("FABRIC", "Fabric")
            .put("ORCHESTRATOR", "Orchestrator")
            .put("HYPERCONVERGED", "Hyperconverged")
            .build();

    static final String TARGET = "Target";

    private static final ZoneOffset ZONE_OFFSET = OffsetDateTime.now().getOffset();

    /**
     * Target status set that contains non-failed validation status from topology processor, including
     * "Validated", "Validation in progress" and "Discovery in progress".
     */
    private static final Set<String> NON_VALIDATION_FAILED_STATUS_SET = ImmutableSet.of(
        StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS,
        StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS,
        StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);

    private final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    private final Duration targetValidationTimeout;

    private final Duration targetValidationPollInterval;

    private final Duration targetDiscoveryTimeout;

    private final Duration targetDiscoveryPollInterval;

    private final LicenseCheckClient licenseCheckClient;

    private final ApiComponentTargetListener apiComponentTargetListener;

    private final ActionSpecMapper actionSpecMapper;

    private final ActionSearchUtil actionSearchUtil;

    private final ApiWebsocketHandler apiWebsocketHandler;

    private final RepositoryApi repositoryApi;

    private final boolean allowTargetManagement;

    public TargetsService(@Nonnull final TopologyProcessor topologyProcessor,
            @Nonnull final Duration targetValidationTimeout,
            @Nonnull final Duration targetValidationPollInterval,
            @Nonnull final Duration targetDiscoveryTimeout,
            @Nonnull final Duration targetDiscoveryPollInterval,
            @Nullable final LicenseCheckClient licenseCheckClient,
            @Nonnull final ApiComponentTargetListener apiComponentTargetListener,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final ActionSpecMapper actionSpecMapper,
            @Nonnull final ActionSearchUtil actionSearchUtil,
            final long realtimeTopologyContextId,
            @Nonnull final ApiWebsocketHandler apiWebsocketHandler,
            final boolean allowTargetManagement) {
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.targetValidationTimeout = Objects.requireNonNull(targetValidationTimeout);
        this.targetValidationPollInterval = Objects.requireNonNull(targetValidationPollInterval);
        this.targetDiscoveryTimeout = Objects.requireNonNull(targetDiscoveryTimeout);
        this.targetDiscoveryPollInterval = Objects.requireNonNull(targetDiscoveryPollInterval);
        this.licenseCheckClient = licenseCheckClient;
        this.apiComponentTargetListener = Objects.requireNonNull(apiComponentTargetListener);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
        this.apiWebsocketHandler = Objects.requireNonNull(apiWebsocketHandler);
        this.allowTargetManagement = allowTargetManagement;
        logger.debug("Created TargetsService with topology processor instance {}",
                topologyProcessor);
    }

    /**
     * Return information about the all known targets. This largely contains information from the
     * associated probe for each target. This is a blocking call that makes network requests of the
     * Topology-Processor.
     *
     * @param environmentType optional filter according to an environment type:
     *                        <ul>
     *                        <li>if this parameter is {@code null} or {@link EnvironmentType#HYBRID},
     *                            no filtering happens (all targets are returned).</li>
     *                        <li>if this parameter is {@link EnvironmentType#UNKNOWN},
     *                            no target is returned.</li>
     *                        <li>if this parameter is {@link EnvironmentType#CLOUD},
     *                            only targets coming from public cloud probes are returned</li>
     *                        <li>if this parameter is {@link EnvironmentType#ONPREM},
     *                            only targets <i>not</i> coming from public cloud probes are returned</li>
     *                        </ul>
     * @return a List of {@link TargetApiDTO} containing the uuid of the target, plus the info from
     *         the related probe.
     */
    @Override
    @Nonnull
    public List<TargetApiDTO> getTargets(@Nullable final EnvironmentType environmentType) {
        try {
            final Set<TargetInfo> targets = topologyProcessor.getAllTargets();
            final Map<Long, ProbeInfo> probeMap = getProbeIdToProbeInfoMap();
            return targets.stream()
                .filter(target -> environmentTypeFilter(target, environmentType, probeMap))
                .filter(target -> !target.isHidden())
                .map(targetInfo -> createTargetDtoWithRelationships(targetInfo, targets, probeMap))
                .collect(Collectors.toList());
        } catch (CommunicationException e) {
            throw new RuntimeException("Error getting targets list", e);
        }
    }

    /**
     * Check if a target matches an environmentType.
     *
     * @param target target to check
     * @param envType environment type
     * @param probeMap map of probeInfo objects indexed by probeId
     * @return true if target matches environmentType, false otherwise
     */
    private boolean environmentTypeFilter(final TargetInfo target, EnvironmentType envType, @Nonnull final Map<Long, ProbeInfo> probeMap) {
        try {
            if (envType == EnvironmentType.CLOUD || envType == EnvironmentType.ONPREM) {
                // gather the other info for this target, based on the related probe
                final long probeId = target.getProbeId();
                final ProbeInfo probeInfo = probeMap.get(probeId);
                if (probeInfo == null) {
                    return false;
                }
                boolean isPublicCloud = GroupMapper.CLOUD_ENVIRONMENT_PROBE_TYPES.contains(probeInfo.getType());

                // return true if and only if isPublicCloud agrees with the parameter
                // this means:
                    // if envType is CLOUD then return all targets coming from public cloud probes
                    // if envType is ONPREM then return all other targets
                return isPublicCloud == (envType == EnvironmentType.CLOUD);
            } else {
                // if the parameter is not there, or if it is HYBRID, no filtering should happen
                // if the parameter is UNKNOWN, all targets should be filtered out
                return envType == null || envType == EnvironmentType.HYBRID;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error setting environment filters", e);
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
     */
    @Override
    @Nonnull
    public TargetApiDTO getTarget(@PathVariable("uuid") String uuid) throws UnknownObjectException {
        logger.debug("Get target {}", uuid);
        // assumes target uuid's are long's in XL
        long targetId = Long.valueOf(uuid);
        try {
            return createTargetDtoWithRelationships(
                    topologyProcessor.getTarget(targetId), Collections.emptySet(), getProbeIdToProbeInfoMap());
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
     */
    @Override
    @Nonnull
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
                if (isProbeLicensed(probeInfo) && isProbeExternallyVisible(probeInfo)) {
                    answer.add(mapProbeInfoToDTO(probeInfo));
                }
            } catch (FieldVerificationException e) {
                throw new RuntimeException(
                        "Fields of target " + probeInfo.getType() + " failed validation", e);
            }
        }
        return answer;
    }

    private boolean isProbeExternallyVisible(ProbeInfo probeInfo) {
        switch (probeInfo.getCreationMode()) {
            case DERIVED:
            case INTERNAL:
                return false;
            default:
                return true;
        }
    }

    /**
     * Is the probe licensed for use? A probe is considered "licensed" if the installed license(s)
     * include the feature(s) necessary to operate the probe. A storage probe, for example, requires
     * the "storage" feature to available in the license.
     *
     * This restriction check is only performed if a {@link LicenseCheckClient} has been provided.
     * If there is no <code>LicenseCheckClient</code> configured, this check always returns
     * <code>true</code>.
     *
     * @param probeInfo the ProbeInfo to check.
     * @return false, if the <code>ProbeController</code> was configured with a
     * <code>LicenseCheckClient</code>, and the probe is <b>not</b> available according to the
     * license. true, otherwise.
     */
    private boolean isProbeLicensed(ProbeInfo probeInfo) {
        if (licenseCheckClient == null) return true;
        if (!probeInfo.getLicense().isPresent()) return true;
        Optional<ProbeLicense> licenseCategory = ProbeLicense.create(probeInfo.getLicense().get());
        return licenseCategory.map(licenseCheckClient::isFeatureAvailable).orElse(true);
    }

    /**
     * Return information about all the Actions related to the entities discovered by
     * the given target.
     *
     * @param uuid the unique ID for the target
     * @param actionApiInputDTO dto used to filter the list of Actions
     * @return a List of {@link ActionApiDTO}
     * @throws Exception should not happen
     */
    @Override
    public List<ActionApiDTO> getActionsByTargetUuid(final String uuid, final ActionApiInputDTO actionApiInputDTO)
            throws Exception {
        final Set<Long> uuidSet = getTargetEntityIds(Long.valueOf(uuid));
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
                                            actionApiInputDTO, Optional.of(uuidSet), null);
        return actionSearchUtil.callActionServiceWithNoPagination(filter);
    }

    /**
     * Return information about all the Entities discovered by the given target.
     *
     * @param uuid the unique ID for the target
     * @return a List of {@link ServiceEntityApiDTO}
     * @throws Exception
     */
    @Override
    public List<ServiceEntityApiDTO> getEntitiesByTargetUuid(final String uuid) throws Exception {
        return getTargetEntities(getTarget(uuid));
    }

    /**
     * Return information about all the Statistics related to the entities discovered by
     * the given target.
     *
     * @param uuid the unique ID for the target
     * @param statPeriodApiInputDTO dto used to filter the list of Statistics
     * @return a List of {@link StatSnapshotApiDTO}
     * @throws Exception
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByTargetQuery(final String uuid, final StatPeriodApiInputDTO statPeriodApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    @Override
    public TargetApiDTO createTarget(@Nonnull String probeType,
            @Nonnull Collection<InputFieldApiDTO> inputFields)
            throws OperationFailedException, InterruptedException, InvalidOperationException {
        logger.debug("Add target {}", probeType);
        Objects.requireNonNull(probeType);
        Objects.requireNonNull(inputFields);
        Preconditions.checkState(allowTargetManagement,
                "Targets management public APIs are not allowed in integration mode");

        // create a target
        try {
            final Collection<ProbeInfo> probes = topologyProcessor.getAllProbes();
            final ProbeInfo probeInfo = probes.stream()
                    .filter(pr -> probeType.equals(pr.getType())).findFirst()
                    .orElseThrow(() -> new OperationFailedException("Could not find probe by type " + probeType));
            if (probeInfo.getCreationMode() == CreationMode.DERIVED) {
                throw new InvalidOperationException(
                        "Derived targets cannot be created through public APIs.");
            }
            final long probeId = probeInfo.getId();
            final TargetData newtargetData = new NewTargetData(probeType, inputFields);
            try {
                // store the target number to determine if to send notification to UI.
                final int targetSizeBeforeValidation = topologyProcessor.getAllTargets().size() ;
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

                // if validation is successful and this is the first target, listen to the results
                // and send notification to UI
                if (validatedTargetInfo != null &&
                        StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS.equals(validatedTargetInfo.getStatus()) &&
                        targetSizeBeforeValidation == 0) {
                    apiComponentTargetListener.triggerBroadcastAfterNextDiscovery();
                }
                return createTargetDtoWithRelationships(
                        validatedTargetInfo, Collections.emptySet(), getProbeIdToProbeInfoMap());
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
    @Nonnull
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
                    targetDto.getStatus().equals(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS))
                .orElse(true);

            if (shouldDiscover) {
                result = Optional.of(discoverTargetSynchronously(targetId));
            }

            if (result.isPresent()) {
                return createTargetDtoWithRelationships(result.get(), Collections.emptySet(), getProbeIdToProbeInfoMap());
            } else {
                return createTargetDtoWithRelationships(
                        topologyProcessor.getTarget(targetId), Collections.emptySet(), getProbeIdToProbeInfoMap());
            }
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
     * @throws InvalidOperationException in case the operation is invalid.
     */
    @Nonnull
    @Override
    public TargetApiDTO editTarget(@Nonnull String uuid,
            @Nonnull Collection<InputFieldApiDTO> inputFields)
            throws OperationFailedException, UnauthorizedObjectException, InterruptedException,
            InvalidOperationException {
        logger.debug("Edit target {}", uuid);
        Preconditions.checkState(allowTargetManagement,
                "Targets management public APIs are not allowed in integration mode");
        long targetId = Long.valueOf(uuid);
        final TargetData updatedTargetData = new NewTargetData(inputFields);
        try {
            TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
            if (targetInfo.isReadOnly()) {
                throw new InvalidOperationException("Read-only target "
                        + targetInfo.getDisplayName() + " (id " + targetId
                        + ") cannot be changed through public APIs.");
            }
            topologyProcessor.modifyTarget(targetId, updatedTargetData);
            return createTargetDtoWithRelationships(targetInfo, Collections.emptySet(), getProbeIdToProbeInfoMap());
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
     * @throws InvalidOperationException in case the operation is invalid.
     */
    @Override
    public void deleteTarget(@Nonnull String uuid)
            throws UnknownObjectException, InvalidOperationException {
        logger.debug("Delete target {}", uuid);
        long targetId = Long.valueOf(uuid);
        Preconditions.checkState(allowTargetManagement,
                "Targets management public APIs are not allowed in integration mode");
        try {
            TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
            if (targetInfo.isReadOnly()) {
                throw new InvalidOperationException("Read-only target "
                        + targetInfo.getDisplayName() + " (id " + targetId
                        + ") cannot be removed through public APIs.");
            }
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
     * Populate a Map from target id to target info for all known targets in order to facilitate lookup
     * by target id.
     *
     * @param allTargetInfos All targets in scope that returned from Topology-Processor in case we
     * made an API call, otherwise an empty set.
     * @return a Map from target Id to targetInfo for all known targets.
     * @throws CommunicationException if there is a problem making the REST API call
     */
    private Map<String, TargetInfo> getTargetIdToTargetInfoMap(
                                    @Nonnull final Set<TargetInfo> allTargetInfos)
            throws CommunicationException {
        Set<TargetInfo> scopedTargetInfos = allTargetInfos.isEmpty() ?
                topologyProcessor.getAllTargets() : allTargetInfos;
        return Maps.uniqueIndex(
                scopedTargetInfos, targetInfo -> String.valueOf(targetInfo.getId()));
    }

    /**
     * Define a class to hold the TargetData used to call targetsRestClient.addTarget(). There is
     * currently no convenient way to do that...but there is a JIRA item OM-10644 to remedy this.
     */
    private static class NewTargetData implements TargetData {

        private static final String IS_STORAGE_BROWSING_ENABLED = "isStorageBrowsingEnabled";
        private final Set<AccountValue> accountData;
        // Is it a VC probe? And is VC storage browsing filed NOT set?
        private final BiFunction<String, Collection<InputFieldApiDTO>, Boolean> isVCStorageBrowsingNotSet =
            (probeType, inputFields) -> SDKProbeType.VCENTER.getProbeType().equals(probeType) &&
                !inputFields.stream().anyMatch(field -> IS_STORAGE_BROWSING_ENABLED.equals(field.getName()));

        private static final InputFieldApiDTO storageBrowsingDisabledDTO = new InputFieldApiDTO();
        static {
            storageBrowsingDisabledDTO.setName(IS_STORAGE_BROWSING_ENABLED);
            storageBrowsingDisabledDTO.setValue("false");
        }

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

        /**
         * Initialize the accountData Set from a List of InputFieldApiDTO's. We use the inputField name
         * and value to build the new AccountData value to describe the target.
         * <p>
         * Note: "isStorageBrowsingEnabled" filed is introduce recently, and if users continue to use
         * old API script to create VC target (without specifying this filed), VC browsing will be enabled by default.
         * But UI shows VC browsing disabled (since target info doesn't have this filed). To address this problem,
         * if this filed is not set when creating VC target, it will be added with value set to "false".
         * So probe and UI are consistent.
         * </p>
         * <p>Warning: The GroupProperties of the InputFieldApiDTO is ignored for now, since there is a
         * mismatch between the InputFieldApiDTO ({@code List<String>)}) and the
         * InputField.groupProperties ({@code List<List<String>>}).
         *
         * @param probeType probe type
         * @param inputFields a list of {@ref InputFieldApiDTO} to use to initialize the accountData
         */
        NewTargetData(@Nonnull final String probeType, final Collection<InputFieldApiDTO> inputFields) {
            final Set<InputFieldApiDTO> inputFieldApiDTOList;
            if (isVCStorageBrowsingNotSet.apply(probeType, inputFields)) {
                // if input fields don't include the newly added "isStorageBrowsingEnabled" filed,
                // e.g. calling from API, set it to "false",  so probe and UI are consistent.
                inputFieldApiDTOList = Sets.newHashSet(inputFields);
                inputFieldApiDTOList.add(storageBrowsingDisabledDTO);
            } else {
                inputFieldApiDTOList = ImmutableSet.copyOf(inputFields);
            }

            accountData = inputFieldApiDTOList.stream().map(inputField -> {
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
        return status == null ?
            UNKNOWN_TARGET_STATUS : TARGET_STATUS_MAP.getOrDefault(status, status);
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
        try {
            topologyProcessor.validateTarget(targetId);
            TargetInfo targetInfo = pollForTargetStatus(targetId, targetValidationTimeout,
                targetValidationPollInterval, StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS);
            // If target status is not validated or in progress from topology processor, the target
            // validation fails. Send failed validation message to UI.
            if (!NON_VALIDATION_FAILED_STATUS_SET.contains(targetInfo.getStatus())) {
                String statusDescription = targetInfo.getStatus() == null ? "Target validation failed." :
                    targetInfo.getStatus();
                TargetNotification targetNotification =
                    buildTargetValidationNotification(targetId, statusDescription);
                apiWebsocketHandler.broadcastTargetValidationNotification(targetNotification);

            }
            return targetInfo;
        } catch (CommunicationException | TopologyProcessorException | InterruptedException e) {
            // If there's any exception when validating the target, send failed validation message to UI.
            TargetNotification targetNotification =
                buildTargetValidationNotification(targetId, "Validation failed due to interruption or communication error.");
             apiWebsocketHandler.broadcastTargetValidationNotification(targetNotification);
            throw e;
        }

    }

    /**
     * Build a TargetNotification with NOT_VALIDATED status and failed validation description.
     *
     * @param targetId          The ID of the target.
     * @param statusDescription Description of the failed validation.
     * @return Notification related to a single target with NOT_VALIDATED status.
     */
    private TargetNotification buildTargetValidationNotification(final long targetId,
                                                                 @Nonnull String statusDescription) {
        TargetStatusNotification statusNotification = TargetStatusNotification.newBuilder()
            .setStatus(TargetStatus.NOT_VALIDATED)
            .setDescription(statusDescription)
            .build();
        return TargetNotification.newBuilder()
            .setStatusNotification(statusNotification)
            .setTargetId(String.valueOf(targetId))
            .build();
    }

    /**
     * Poll the topology processor until discovery completes or the discovery times out.
     * Execution of this method blocks until one of the following happens:
     * <ul>
     *     <li>Discovery completes.</ul>
     *     <li>An exception is thrown.</ul>
     *     <li>The discovery timeout expires.</ul>
     * </ul>
     *
     * If the timeout expires, the method returns the status of target when the timeout expires.
     *
     * @param targetId The ID of the target to be discovered.
     * @return The discovery status of the target upon completion or when the timeout expires.
     * @throws CommunicationException if there is an HTTP error.
     * @throws TopologyProcessorException if the update fails in the T-P.
     * @throws InterruptedException If the polling is interrupted.
     */
    @Nonnull
    @VisibleForTesting
    TargetInfo discoverTargetSynchronously(final long targetId)
        throws CommunicationException, TopologyProcessorException, InterruptedException {

        topologyProcessor.discoverTarget(targetId);
        return pollForTargetStatus(targetId, targetDiscoveryTimeout,
                targetDiscoveryPollInterval, StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);
    }

    /**
     * @param targetId The ID of the target to be polled.
     * @param timeout Timeout for the operation.
     * @param pollInterval Wait time between polling.
     * @param waitOnStatus Status string on which to wait on.
     * @return The discovery status of the target upon completion or when the timeout expires.
     * @throws CommunicationException if there is an HTTP error.
     * @throws TopologyProcessorException if the update fails in the T-P.
     * @throws InterruptedException If the polling is interrupted.
     */
    private TargetInfo pollForTargetStatus(final long targetId,
                                           final Duration timeout,
                                           final Duration pollInterval,
                                           String waitOnStatus)
        throws CommunicationException, TopologyProcessorException, InterruptedException {

        TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
        Duration elapsed = Duration.ofMillis(0);

        while (targetInfo.getStatus() != null &&
            targetInfo.getStatus().equals(waitOnStatus) &&
            elapsed.compareTo(timeout) < 0) {

            Thread.sleep(pollInterval.toMillis());
            elapsed = elapsed.plus(pollInterval);
            targetInfo = topologyProcessor.getTarget(targetId);
            logger.debug("Polled status of \"{}\" after waiting {}s for target {}",
                targetInfo.getStatus(), elapsed.getSeconds(), targetId);
        }

        return targetInfo;
    }

    /**
     * Creates a {@link TargetApiDTO} instance from information in a {@link TargetInfo} object
     * by mapProbeInfoToDTO() and sets the "derived targets" attribute of the target.
     *
     * @param targetInfo the {@link TargetInfo} structure returned from the Topology-Processor.
     * @param allTargetInfos All targets in scope that returned from Topology-Processor in case we
     * made an API call, otherwise an empty set.
     * @param probeMap A map of probeInfo indexed by probeId.
     * @return a {@link TargetApiDTO} containing the target information and its derived targets
     * relationships.
     */
    private TargetApiDTO createTargetDtoWithRelationships(@Nonnull final TargetInfo targetInfo,
                                                 @Nonnull final Set<TargetInfo> allTargetInfos,
                                                 @Nonnull final Map<Long, ProbeInfo> probeMap) {

        try {
            TargetApiDTO targetApiDTO = mapTargetInfoToDTO(targetInfo, probeMap);
            targetApiDTO.setDerivedTargets(
                convertDerivedTargetInfosToDtos(targetInfo.getDerivedTargetIds().stream()
                    .map(String::valueOf).collect(Collectors.toList()), allTargetInfos, probeMap));
            return targetApiDTO;
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
    }

    /**
     * Creates a List of {@link TargetApiDTO}s to be set as the derived targets of
     * their parent target.
     *
     * @param derivedTargetIds a List of derived target's ids that associated with a parent target.
     * @param allTargetInfos All targets in scope that returned from Topology-Processor in case we
     * made an API call, otherwise an empty set.
     * @param probeMap A map of probeInfo objects indexed by probeId.
     * @return List of {@link TargetApiDTO}s containing the target information from their
     * {@link TargetInfo}s.
     */
    private List<TargetApiDTO> convertDerivedTargetInfosToDtos(
                                @Nonnull final List<String> derivedTargetIds,
                                @Nonnull final Set<TargetInfo> allTargetInfos,
                                @Nonnull final Map<Long, ProbeInfo> probeMap) {
        if (derivedTargetIds.isEmpty()) {
            return Collections.emptyList();
        }
        final Map<String, TargetInfo> targetInfosByTargetId;
        try {
            targetInfosByTargetId = getTargetIdToTargetInfoMap(allTargetInfos);
        } catch (CommunicationException e) {
            throw new CommunicationError(e);
        }
        List<TargetApiDTO> derivedTargetsDtos = Lists.newArrayList();
        derivedTargetIds.forEach(targetId -> {
            TargetInfo targetInfo = targetInfosByTargetId.get(targetId);
            if (targetInfo != null) {
                if (targetInfo.isHidden()) {
                    logger.debug("Skip the conversion of a hidden derived target: {}", targetId);
                } else {
                    try {
                        derivedTargetsDtos.add(mapTargetInfoToDTO(targetInfo, probeMap));
                    } catch (CommunicationException e) {
                        throw new CommunicationError(e);
                    }
                }
            } else {
                logger.warn(
                    "Derived Target {} no longer exists, but appears as a derived target in the " +
                            "target store", targetId);
            }
        });
        return derivedTargetsDtos;
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
     */
    private TargetApiDTO mapTargetInfoToDTO(@Nonnull final TargetInfo targetInfo,
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
                logger.error("AccountDefEntry not found for {} in probe with ID: {}",
                        errorFields, probeInfo.getId());
                throw new RuntimeException("AccountDef Entry not found for "
                        + errorFields + " in probe with id: " + probeInfo.getId());
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
            case LIST:
                return InputValueType.LIST;
            default:
                throw new RuntimeException("Unrecognized account field value type: " + type);
        }
    }

    /**
     * Probe category strings as defined by the difference probe_conf.xml are not consistent
     * regarding uppercase/lowercase, etc. This maps the known problem category strings into
     * more user-friendly names.
     *
     * @param category the probe category
     * @return a user-friendly string for the probe category (for the problem categories we know of).
     */
    private String getUserFacingCategoryString(final String category) {
        return USER_FACING_CATEGORY_MAP.getOrDefault(category, category);
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

    /**
     * Gets the entities that belong to the given target
     * @param targetDTO The TargetApiDTO object
     * @return A list of ServiceEntityApiDTO of the target's entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    private List<ServiceEntityApiDTO> getTargetEntities(TargetApiDTO targetDTO)
                throws ConversionException, InterruptedException {
        final String targetUuid = targetDTO.getUuid();
        final String targetType = targetDTO.getType();

        // targetIdToProbeType is needed to fill discoveredBy attribute of the entities.
        Map<Long, String> targetIdToProbeType = new HashMap<>();
        targetIdToProbeType.put(Long.parseLong(targetUuid), targetType);
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.discoveredBy(Long.parseLong(targetUuid)))
                .build())
            .getSEList();
    }

    @Nonnull
    private Set<Long> getTargetEntityIds(long targetId) {
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                                                    SearchProtoUtil.discoveredBy(targetId))
                                                .build())
                        .getOids();
    }
}
