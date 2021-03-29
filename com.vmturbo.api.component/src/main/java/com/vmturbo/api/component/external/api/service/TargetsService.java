package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMMUNICATION_BINDING_CHANNEL;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;

import com.vmturbo.api.TargetNotificationDTO.TargetNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification.TargetStatus;
import com.vmturbo.api.component.communication.ApiComponentTargetListener;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.SearchOrderByMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.target.TargetMapper;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.target.TargetHealthApiDTO;
import com.vmturbo.api.dto.workflow.WorkflowApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.HealthState;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.PaginationUtil;
import com.vmturbo.api.serviceinterfaces.ITargetsService;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeaturesRequiredException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.ITargetHealthInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

/**
 * Service entry points to search the Repository.
 **/
public class TargetsService implements ITargetsService {

    static final String TARGET = "Target";

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

    private final int apiPaginationDefaultLimit;

    /**
     * Used to map a ProbeInfo to a TargetApiDTO.
     */
    private final TargetMapper targetMapper = new TargetMapper();

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
                          @Nonnull final ApiWebsocketHandler apiWebsocketHandler,
                          final boolean allowTargetManagement,
                          final int apiPaginationDefaultLimit) {
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
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit;
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
                    answer.add(targetMapper.mapProbeInfoToDTO(probeInfo));
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
     * @param targetUuid the unique ID for the target
     * @param cursor - index to indicate where to start the results
     * @param limit - Maximum number of items to return after the cursor
     * @param searchOrderBy - field used to order the results
     * @param ascending - ascending order
     * @return {@link ResponseEntity} with collection of ServiceEntityApiDTO
     * @throws Exception error while processing request
     */
    @Override
    public ResponseEntity<List<ServiceEntityApiDTO>> getEntitiesByTargetUuid(final String targetUuid,
                                                                             @Nullable final String cursor,
                                                                             @Nullable final Integer limit,
                                                                             @Nullable final Boolean ascending,
                                                                             @Nullable final String searchOrderBy) throws Exception {

        Search.SearchParameters build = SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.discoveredBy(Long.parseLong(targetUuid)))
                .build();

        RepositoryApi.SearchRequest searchRequest = repositoryApi.newSearchRequest(build);
        /* supports backwards compatibility for non-pagination calls.
         * Use of any of the pagination-related params here will explicitly trigger a pagination response
         */
        if (limit == null && cursor == null & ascending == null && searchOrderBy == null) {
            //UNPAGINATED CALLS
            List<ServiceEntityApiDTO> entities = searchRequest.getSEList();
            return PaginationUtil.buildResponseEntity(entities, null, null, null);
        }

        //PAGINATED CALLS
        final SearchOrderBy protoSearchOrderByEnum = searchOrderBy == null ? SearchOrderBy.ENTITY_NAME :
                SearchOrderByMapper.fromApiToProtoEnum(
                        com.vmturbo.api.pagination.SearchOrderBy.valueOf(searchOrderBy.toUpperCase()));
        Pagination.OrderBy orderBy = Pagination.OrderBy.newBuilder()
                .setSearch(protoSearchOrderByEnum)
                .build();

        Pagination.PaginationParameters.Builder paginationParameters = Pagination.PaginationParameters.newBuilder()
                .setLimit(limit == null ? this.apiPaginationDefaultLimit : limit)
                .setAscending(ascending == null ? true : ascending)
                .setOrderBy(orderBy);

        if (cursor != null) {
            paginationParameters.setCursor(cursor);
        }

        return searchRequest.getPaginatedSEList(paginationParameters.build());
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
        // Check if the probe type is licensed. If it's not then runtime exception will be thrown.
        checkLicenseByProbeType(probeType);
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
        // Check if the probe type is licensed. If it's not then runtime exception will be thrown.
        checkLicenseByTargetUuid(uuid);
        Preconditions.checkState(allowTargetManagement,
                "Targets management public APIs are not allowed in integration mode");
        long targetId = Long.valueOf(uuid);
        final NewTargetData updatedTargetData = new NewTargetData(inputFields);
        try {
            TargetInfo targetInfo = topologyProcessor.getTarget(targetId);
            if (targetInfo.isReadOnly()) {
                throw new InvalidOperationException("Read-only target "
                        + targetInfo.getDisplayName() + " (id " + targetId
                        + ") cannot be changed through public APIs.");
            }
            topologyProcessor.modifyTarget(targetId,
                    new TargetInputFields(updatedTargetData.inputFieldsList, updatedTargetData.getCommunicationBindingChannel()));
            TargetInfo updatedTargetInfo = topologyProcessor.getTarget(targetId);
            return createTargetDtoWithRelationships(updatedTargetInfo, Collections.emptySet(), getProbeIdToProbeInfoMap());
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
        private final List<InputField> inputFieldsList;
        private Optional<String> communicationBindingChannel = Optional.empty();

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
            inputFieldsList = new ArrayList<>();
            inputFields.forEach(inputField -> {
                if (inputField.getName().equals(COMMUNICATION_BINDING_CHANNEL)) {
                    this.communicationBindingChannel = Optional.ofNullable(inputField.getValue());
                } else {
                    inputFieldsList.add(new InputField(inputField.getName(),
                            inputField.getValue(),
                            // TODO: fix type mismatch between InputFieldApiDTO.groupProperties and
                            // TargetRESTAPI.TargetSpec.InputField.groupProperties
                            // until then...return empty Optional
                            Optional.empty()));
                }
            });
        }

        /**
         * Initialize the accountData Set from a List of InputFieldApiDTO's. We use the inputField name
         * and value to build the new AccountData value to describe the target.
         * <p>
         * <p>Warning: The GroupProperties of the InputFieldApiDTO is ignored for now, since there is a
         * mismatch between the InputFieldApiDTO ({@code List<String>)}) and the
         * InputField.groupProperties ({@code List<List<String>>}).
         *
         * @param probeType probe type
         * @param inputFields a list of {@ref InputFieldApiDTO} to use to initialize the accountData
         */
        NewTargetData(@Nonnull final String probeType, final Collection<InputFieldApiDTO> inputFields) {
            this(ensureStorageBrowsingFlagSet(probeType, inputFields));
        }

        @Nonnull
        @Override
        public Set<AccountValue> getAccountData() {
            return new HashSet<>(inputFieldsList);
        }

        @Override
        public Optional<String> getCommunicationBindingChannel() {
            return communicationBindingChannel;
        }

        /**
         * Parses the input fields before initializing them.
         * Note:"isStorageBrowsingEnabled" field is introduce recently, and if users continue to use
         * old API script to create VC target (without specifying this field), VC browsing will be
         * enabled by default.
         * But UI shows VC browsing disabled (since target info doesn't have this field). To
         * address this problem, if this field is not set when creating VC target, it will be
         * added with value set to "false".
         * So probe and UI are consistent.
         *
         * @param probeType probe type
         * @param inputFieldsApiDTO a list of {@ref InputFieldApiDTO} to use to initialize the accountData
         * @return a list of parsed {@link InputFieldApiDTO}
         */
        private static Collection<InputFieldApiDTO> ensureStorageBrowsingFlagSet(@Nonnull final String probeType, final Collection<InputFieldApiDTO> inputFieldsApiDTO) {
            // Is it a VC probe? And is VC storage browsing field NOT set?
            final BiFunction<String, Collection<InputFieldApiDTO>, Boolean> isVCStorageBrowsingNotSet =
                    (probe, inputFields) -> SDKProbeType.VCENTER.getProbeType().equals(probeType)
                            && inputFields.stream().noneMatch(field -> IS_STORAGE_BROWSING_ENABLED.equals(field.getName()));
            final Set<InputFieldApiDTO> inputFieldApiDTOList;
            if (isVCStorageBrowsingNotSet.apply(probeType, inputFieldsApiDTO)) {
                // if input fields don't include   the newly added "isStorageBrowsingEnabled" field,
                // e.g. calling from API, set it to "false",  so probe and UI are consistent.
                inputFieldApiDTOList = Sets.newHashSet(inputFieldsApiDTO);
                inputFieldApiDTOList.add(storageBrowsingDisabledDTO);
            } else {
                inputFieldApiDTOList = ImmutableSet.copyOf(inputFieldsApiDTO);
            }
            return inputFieldApiDTOList;
        }
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
            TargetApiDTO targetApiDTO = targetMapper.mapTargetInfoToDTO(targetInfo, probeMap);
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
                        derivedTargetsDtos.add(targetMapper.mapTargetInfoToDTO(targetInfo, probeMap));
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

    @Nonnull
    private Set<Long> getTargetEntityIds(long targetId) {
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.discoveredBy(targetId))
                .build())
                .getOids();
    }

    @Override
    @Nonnull
    public List<TargetHealthApiDTO> getTargetsHealth(@Nullable HealthState state) {
        //FIXME implement
        throw new UnsupportedOperationException();
    }

    @Override
    @Nonnull
    public TargetHealthApiDTO getHealthByTargetUuid(@Nonnull String uuid) {
        long targetId = Long.parseLong(uuid);
        try {
            ITargetHealthInfo healthInfo = topologyProcessor.getTargetHealth(targetId);
            return targetMapper.mapTargetHealthInfoToDTO(healthInfo);
        } catch (CommunicationException | TopologyProcessorException e) {
            throw new RuntimeException("Error getting target health info", e);
        }
    }

    /**
     * Check if probe type of the target is licensed. If it's not, then runtime
     * {@link LicenseFeaturesRequiredException} will be thrown.
     *
     * @param uuid the uuid of the target to be validated
     * @throws LicenseFeaturesRequiredException if target probe type is not licensed
     */
    private void checkLicenseByTargetUuid(String uuid) throws LicenseFeaturesRequiredException {
        try {
            final TargetApiDTO target = getTarget(uuid);
            checkLicenseByProbeType(target.getType());
        } catch (UnknownObjectException e) {
            logger.error("Failed to check license for target " + uuid, e);
        }
    }

    /**
     * Check if probe type is licensed. If it's not then runtime
     * {@link LicenseFeaturesRequiredException} will be thrown.
     *
     * @param probeType probe type
     * @throws LicenseFeaturesRequiredException if probe type is not licensed
     */
    private void checkLicenseByProbeType(String probeType) throws LicenseFeaturesRequiredException {
        try {
            topologyProcessor.getAllProbes().stream()
                            .filter(p -> p.getType().equals(probeType))
                            .findAny()
                            .flatMap(ProbeInfo::getLicense)
                            .flatMap(ProbeLicense::create)
                            .ifPresent(licenseCheckClient::checkFeatureAvailable);
        } catch (CommunicationException e) {
            logger.error("Failed to check license for probe type " + probeType, e);
        }
    }
}
