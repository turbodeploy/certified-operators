package com.vmturbo.action.orchestrator.execution;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.GetDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.GetDiscoveredGroupsResponse;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.TargetDiscoveredGroups;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * Class to manage creation of policies for VMs that failed to resize.  The current use for this is
 * to disable execution of subsequent actions in an availability set for a period of time after an
 * execution failure has occurred.
 */
public class FailedCloudResizePolicyCreator {
    /**
     * Logger.
     */
    public static final Logger logger = LogManager.getLogger(FailedCloudResizePolicyCreator.class);

    private final GroupServiceBlockingStub groupServiceClient;
    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final ScheduleServiceBlockingStub scheduleService;
    private final RepositoryServiceBlockingStub repositoryService;
    private final DiscoveredGroupServiceBlockingStub discoveredGroupService;
    private final List<Pattern> failedActionPatterns;
    private final Long recommendOnlyPolicyLifetimeMs;

    /**
     * Create a FailedCloudResizePolicyCreator.
     *
     * @param groupServiceClient group service gRPC stub
     * @param settingPolicyService settings policy service gRPC stub
     * @param scheduleService schedule service gRPC stub
     * @param repositoryService repository service gRPC stub
     * @param discoveredGroupService discovered group service gRPC stub
     * @param failedActionPatterns list of action error message strings that will trigger the
     * @param recommendOnlyPolicyLifetimeHours how long a recommend only policy should be active
     *      after a failed availability set action execution.
     */
    public FailedCloudResizePolicyCreator(final GroupServiceBlockingStub groupServiceClient,
            final SettingPolicyServiceBlockingStub settingPolicyService,
            final ScheduleServiceBlockingStub scheduleService,
            final RepositoryServiceBlockingStub repositoryService,
            final DiscoveredGroupServiceBlockingStub discoveredGroupService,
            final String failedActionPatterns, final Long recommendOnlyPolicyLifetimeHours) {
        this.groupServiceClient = groupServiceClient;
        this.settingPolicyService = settingPolicyService;
        this.scheduleService = scheduleService;
        this.repositoryService = repositoryService;
        this.discoveredGroupService = discoveredGroupService;
        // Pre-compile the patterns for efficiency.
        this.failedActionPatterns = Arrays.stream(failedActionPatterns.split("\n"))
                .map(pattern -> {
                    try {
                        return Pattern.compile(pattern);
                    } catch (PatternSyntaxException e) {
                        logger.error(
                                "Regular expression syntax error in failedActionPatterns list: '{}'",
                                pattern);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        // For testing, if the configured value is less than zero, the value represents minutes
        // instead of hours.  We also increase the frequency of the scrubber process.
        TimeUnit scheduleUnit = TimeUnit.HOURS;
        if (recommendOnlyPolicyLifetimeHours < 0) {
            scheduleUnit = TimeUnit.MINUTES;
        }
        this.recommendOnlyPolicyLifetimeMs =
                scheduleUnit.toMillis(Math.abs(recommendOnlyPolicyLifetimeHours));
    }

    /**
     * Create a name for the associated policy.
     *
     * @param availabilitySetName name of availability set
     * @param accountId account ID
     * @return name of the policy. If the resulting name is greater than 255 characters, the excess
     *          characters at the end of the name are compressed into a hash code so that the name
     *          will fix into its database column.
     */
    private String makePolicyName(@Nonnull String availabilitySetName,
            @Nonnull Long accountId) {
        return SDKUtil.fixUuid(String.format(
                "%s - Failed execution recommend only Policy (account %d)",
                availabilitySetName, accountId), 255, 235);
    }

    /**
     * Check for a failed action.  If the action is an execution failure for a VM inside a multi-VM
     * availability set, create and attach a recommend only policy to the availability set group.
     *
     * @param actionView the action details.
     * @param actionFailure The progress notification for an action, which contains the reason for
     *                      the failure.
     */
    public void handleFailedAction(@Nonnull ActionView actionView,
                                          @Nonnull ActionFailure actionFailure) {
        if (recommendOnlyPolicyLifetimeMs == 0L) {
            // Creating recommend only policies is disabled.
            return;
        }
        // Only create the policy if the execution failure matches a pattern in the configuration.
        Action action = actionView.getRecommendation();
        if (!shouldHandleFailure(actionFailure)) {
            logger.debug("Not creating recommend only policy for action {}: Action is successful",
                    action.getId());
            return;
        }

        ActionInfo actionInfo = action.getInfo();
        Long targetOid = null;
        if (actionInfo.hasScale()) {
            targetOid = actionInfo.getScale().getTarget().getId();
        } else if (actionInfo.hasMove()) {
            targetOid = actionInfo.getMove().getTarget().getId();
        }
        if (targetOid == null) {
            logger.warn("Not creating recommend only policy for action {}: Missing move/scale info",
                    action.getId());
            return;
        }

        // If the VM is not a member of a multi-VM availability set, do nothing.
        Optional<String> availabilitySetName = getOwningAvailabilitySet(targetOid, 2);
        if (!availabilitySetName.isPresent()) {
            return;
        }
        TopologyEntityDTO vm = getVM(targetOid);
        if (vm == null) {
            logger.warn("Not creating recommend only policy for action {}: Cannot find VM ID {}",
                    action.getId(), targetOid);
            return;
        }
        Optional<Long> accountId = getAccountId(vm);
        if (!accountId.isPresent()) {
            logger.warn("Not creating recommend only policy for action {}: Cannot find account ID",
                    action.getId());
            return;
        }
        logger.info("Creating recommend only policy for {} due to failed action ID {}",
                availabilitySetName.get(), action.getId());
        createRecommendOnlyPolicy(availabilitySetName.get(), accountId.get());
    }

    /**
     * Locate a setting policy with the given name.
     *
     * @param policyName policy to search for.
     * @return Optional setting policy, or empty if the policy was not found.
     */
    private Optional<SettingPolicy> findPolicy(String policyName) {
        Optional<SettingPolicy> foundPolicy = Optional.empty();
        // Unfortunately, there is no name-based filter, so we need to query all setting policies.
        final ListSettingPoliciesRequest req = ListSettingPoliciesRequest.newBuilder().build();
        Iterator<SettingPolicy> rsp = settingPolicyService.listSettingPolicies(req);
        while (rsp.hasNext()) {
            SettingPolicy settingPolicy = rsp.next();
            if (settingPolicy.getInfo().getDisplayName().equals(policyName)) {
                foundPolicy = Optional.of(settingPolicy);
                break;
            }
        }
        // Need to drain the iterator to avoid a leak
        while (rsp.hasNext()) {
            rsp.next();
        }
        return foundPolicy;
    }

    /**
     * Create a recommend only policy for the provided group and account.  If a policy with the
     * specified name exists or the policy cannot be created, null is returned.
     *
     * @param groupName name of the group that the policy targets
     * @param accountId associated account ID
     */
    private void createRecommendOnlyPolicy(@Nonnull String groupName, Long accountId) {
        logger.info("Locating availability set group {} for recommend only policy", groupName);
        Optional<Grouping> groupInfo = findGroup(groupName);
        if (!groupInfo.isPresent()) {
            logger.warn("Not creating recommend only policy for availability set {}: "
                    + "Cannot find discovered group", groupName);
            return;
        }
        long now = System.currentTimeMillis();
        String policyName = makePolicyName(groupName, accountId);

        // Create or reuse the associated schedule.  The schedule has the same name as the
        // associated policy.
        Optional<Pair<Long, Boolean>> scheduleResult = createOrUpdateSchedule(policyName, now,
                recommendOnlyPolicyLifetimeMs);
        if (!scheduleResult.isPresent()) {
            logger.error("Cannot create/update schedule for policy '{}'", policyName);
            return;
        }
        Long scheduleId = scheduleResult.get().first;

        // If there's an existing policy, update its attached schedule's expiration time.
        Optional<Pair<SettingPolicy, Boolean>> policyResult =
                createOrUpdatePolicy(policyName, groupInfo.get().getId(), scheduleId);

        if (!policyResult.isPresent()) {
            // Could not create or update the policy
            logger.error("Cannot create/update policy '{}'", policyName);
            // If we created a new schedule for this policy, delete it.
            if (scheduleResult.get().second) {
                logger.debug("Deleting newly-created schedule '{}' because associate policy could"
                        + " not be created/updated", policyName);
                try {
                    scheduleService.deleteSchedule(DeleteScheduleRequest.newBuilder()
                            .setOid(scheduleId).build());
                } catch (StatusRuntimeException e) {
                    logger.debug("Failure deleting newly-created schedule '{}': {}", policyName, e);
                }
            }
        }
    }

    /**
     * Create or update a recommend only policy with new schedule information.
     *
     * @param policyName policy name to update
     * @param groupId scope of the policy, in case the policy is created
     * @param scheduleId schedule to attach to the policy
     * @return optional pair containing the created/updated setting policy and whether the policy
     *        was created. If there was an error creating or updating the policy, empty is returned.
     */
    private Optional<Pair<SettingPolicy, Boolean>> createOrUpdatePolicy(String policyName,
            Long groupId, Long scheduleId) {
        // Check for an existing policy
        Optional<SettingPolicy> optExistingPolicy = findPolicy(policyName);
        if (optExistingPolicy.isPresent()) {
            SettingPolicy existingPolicy = optExistingPolicy.get();
            // Modify the existing policy
            logger.info("Policy '{}' exists - updating expiration time on attached schedule",
                    policyName);
            // If the policy has no schedule ID, then set it.  If the schedule ID is the same, do
            // nothing.  If the policy has a schedule ID and it's different, log a warning and do
            // not overwrite it.
            SettingPolicyInfo info = existingPolicy.getInfo();
            if (info.hasScheduleId()) {
                if (info.getScheduleId() == scheduleId) {
                    // Existing policy with matching schedule, so return it.
                    logger.debug("Recommend only policy '{}' already has scheduled ID {} attached",
                            policyName, scheduleId);
                    return Optional.of(new Pair<>(existingPolicy, false));
                }
                // The policy has an unrelated schedule attached to it.  Log a warning and
                // return without modifying the policy.
                logger.warn("Recommend only policy '{}' has unrelated schedule '{}' attached -"
                        + " using existing unrelated schedule", policyName, info.getDisplayName());
                return Optional.empty();
            }
            // Attach the schedule to the existing policy
            UpdateSettingPolicyRequest req = UpdateSettingPolicyRequest.newBuilder()
                    .setId(existingPolicy.getId())
                    .setNewInfo(createSettingPolicyInfo(policyName, info.getScope().getGroupsList(),
                            scheduleId))
                    .build();
            UpdateSettingPolicyResponse rsp = null;
            try {
                rsp = settingPolicyService.updateSettingPolicy(req);
            } catch (StatusRuntimeException e) {
                logger.warn("Unable to update recommend only policy '{}': {}", policyName, e);
            }
            return rsp != null
                    ? Optional.of(new Pair<>(rsp.getSettingPolicy(), false))
                    : Optional.empty();
        }

        // New policy
        SettingPolicyInfo.Builder info = createSettingPolicyInfo(policyName,
                ImmutableList.of(groupId), scheduleId);
        CreateSettingPolicyRequest createReq =
                CreateSettingPolicyRequest.newBuilder().setSettingPolicyInfo(info).build();
        CreateSettingPolicyResponse rsp = null;
        try {
            rsp = settingPolicyService.createSettingPolicy(createReq);
        } catch (StatusRuntimeException e) {
            logger.warn("Exception while creating recommend only policy '{}': {}", policyName, e);
        }
        if (rsp == null || !rsp.hasSettingPolicy()) {
            // Could not create the policy
            logger.warn("Unable to create recommend only policy '{}'", policyName);
            return Optional.empty();
        }
        return Optional.of(new Pair<>(rsp.getSettingPolicy(), true));
    }

    /**
     * Locate the Azure availability set that contains the target VM.
     *
     * @param targetOid OID of the target VM to search
     * @param minimumEntities Only return a result if the availability set contains at least this
     *                        many VMs.
     * @return Optional containing the availability set that owns the target OID, else empty.
     *
     */
    private Optional<String> getOwningAvailabilitySet(Long targetOid, int minimumEntities) {
        GetDiscoveredGroupsRequest request = GetDiscoveredGroupsRequest.newBuilder().build();
        GetDiscoveredGroupsResponse response = discoveredGroupService.getDiscoveredGroups(request);
        for (TargetDiscoveredGroups targetDiscoveredGroups : response.getGroupsByTargetIdMap().values()) {
            for (DiscoveredGroupInfo discoveredGroupInfo : targetDiscoveredGroups.getGroupList()) {
                GroupDefinition uploadedGroup = discoveredGroupInfo.getUploadedGroup().getDefinition();
                if (!uploadedGroup.getDisplayName().startsWith("AvailabilitySet::")) {
                    continue;
                }
                List<StaticMembersByType> membersByTypes =
                        uploadedGroup.getStaticGroupMembers().getMembersByTypeList();
                for (StaticMembersByType staticMembersByType : membersByTypes) {
                    if (!staticMembersByType.getType().hasEntity()) {
                        continue;
                    }
                    if (staticMembersByType.getType().getEntity() != EntityType.VIRTUAL_MACHINE.getValue()) {
                        continue;
                    }
                    if (staticMembersByType.getMembersCount() < minimumEntities) {
                        continue;
                    }
                    for (Long oid : staticMembersByType.getMembersList()) {
                        if (oid.equals(targetOid)) {
                            return Optional.of(uploadedGroup.getDisplayName());
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Return the VM group with the indicated name.
     *
     * @param groupName name of group to find
     * @return optional of existing group
     */
    private Optional<Grouping> findGroup(String groupName) {
        Iterator<Grouping> groupResponse = groupServiceClient.getGroups(
                GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                .setGroupType(GroupType.REGULAR)
                                .addPropertyFilters(PropertyFilter.newBuilder()
                                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                        .setStringFilter(StringFilter.newBuilder()
                                            .setStringPropertyRegex(SearchProtoUtil
                                                            .escapeSpecialCharactersInLiteral(groupName)))))
                        .build());

        while (groupResponse.hasNext()) {
            Grouping group = groupResponse.next();
            if (GroupProtoUtil.getEntityTypes(group).equals(
                    Collections.singleton(ApiEntityType.VIRTUAL_MACHINE))) {
                return Optional.of(group);
            }
        }
        return Optional.empty();
    }

    @Nullable
    private TopologyEntityDTO getVM(Long targetOid) {
        final List<TopologyEntityDTO> allResults = getVMs(targetOid);
        if (allResults.isEmpty()) {
            logger.warn("Cannot find VM with OID {} - cannot create recommend only policy", targetOid);
            return null;
        }
        if (allResults.size() > 1) {
            logger.warn("Multiple entities with OID {} - cannot create recommend only policy", targetOid);
            return null;
        }
        return allResults.get(0);
    }

    @Nonnull private Schedule.Builder createSchedule(@Nonnull String policyName,
            @Nonnull Long startTime, @Nonnull Long policyLifetime) {
        return Schedule.newBuilder()
                .setDisplayName(policyName)
                .setOneTime(Schedule.OneTime.newBuilder())
                .setStartTime(startTime)
                .setEndTime(startTime + policyLifetime)
                .setTimezoneId(TimeZone.getDefault().getID())
                .setDeleteAfterExpiration(true);
    }

    /**
     * Create or update a policy schedule.
     *
     * @param policyName associated policy name, used to create the schedule name
     * @param startTime start of active window
     * @param policyLifetime length in milliseconds of active window
     * @return optional pair containing the schedule's ID and whether the schedule was created.
     *      True means created, false means the existing schedule was updated.  If the optional
     *      is empty, there was an error creating the schedule. If we were unable to update an
     *      existing schedule, we still return success (i.e., updating the schedule's expiration
     *      time is a best effort operation).
     */
    private Optional<Pair<Long, Boolean>> createOrUpdateSchedule(@Nonnull String policyName,
            long startTime, @Nonnull Long policyLifetime) {
        // Create the schedule info, to be used by the creation or update logic below.
        Schedule.Builder scheduleSetting = createSchedule(policyName, startTime, policyLifetime);

        boolean created = true;
        String operation = "create";
        Long scheduleId = null;
        Iterator<Schedule> getSchedulesResponse = scheduleService.getSchedules(GetSchedulesRequest.newBuilder().build());
        while (getSchedulesResponse.hasNext()) {
            Schedule schedule = getSchedulesResponse.next();
            if (schedule.getDisplayName().equals(policyName)) {
                scheduleId = schedule.getId();
                break;
            }
        }
        // Drain the iterator to avoid leaks
        while (getSchedulesResponse.hasNext()) {
            getSchedulesResponse.next();
        }
        if (scheduleId != null) {
            // The schedule exists, so update its expiration.
            created = false;
            operation = "update";
            UpdateScheduleRequest updateScheduleRequest = UpdateScheduleRequest.newBuilder()
                    .setUpdatedSchedule(scheduleSetting)
                    .setOid(scheduleId)
                    .build();
            scheduleId = null;
            try {
                UpdateScheduleResponse updateScheduleResponse =
                        scheduleService.updateSchedule(updateScheduleRequest);
                if (updateScheduleResponse != null && updateScheduleResponse.hasSchedule()) {
                    scheduleId = updateScheduleRequest.getOid();
                }
            } catch (StatusRuntimeException e) {
                logger.error("Exception while updating schedule '{}': {}", policyName, e);
            }
            if (scheduleId == null) {
                // We had an issue updating the schedule, so we'll need to use the existing one.
                logger.warn("Could not update schedule attached to recommend only policy '{}',"
                                + " using existing schedule", policyName);
            }
        } else {
            CreateScheduleRequest req = CreateScheduleRequest.newBuilder()
                    .setSchedule(scheduleSetting).build();
            CreateScheduleResponse rsp;
            try {
                rsp = scheduleService.createSchedule(req);
                if (rsp.hasSchedule() && rsp.getSchedule().hasId()) {
                    scheduleId = rsp.getSchedule().getId();
                }
                created = true;
            } catch (StatusRuntimeException e) {
                // gRPC error.  Log it and continue with scheduleId == null
                logger.error("Exception while creating schedule '{}': {}", policyName, e);
            }
        }
        if (scheduleId == null) {
            logger.error("Cannot {} schedule for policy '{}'", operation, policyName);
            return Optional.empty();
        }
        return Optional.of(new Pair<>(scheduleId, created));
    }

    @VisibleForTesting
    SettingPolicyInfo.Builder createSettingPolicyInfo(@Nonnull String policyName,
            @Nonnull List<Long> scope, @Nonnull Long scheduleId) {
        Setting.Builder automation = Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.CloudComputeScale.getSettingName())
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(ActionMode.RECOMMEND.toString()));
        return SettingPolicyInfo.newBuilder()
                .setName(StringConstants.AVAILABILITY_SET_RECOMMEND_ONLY_PREFIX + policyName)
                .setDisplayName(policyName)
                .setScheduleId(scheduleId)
                .setDeleteAfterScheduleExpiration(true)
                .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                .setScope(Scope.newBuilder().addAllGroups(scope))
                .addSettings(automation);
    }

    /**
     * Return whether the action failure requires adding a recommend only policy based on the
     * failed action's error description.
     *
     * @param actionFailure failed action to check
     * @return true if the action failure should trigger generation of a recommend only policy to
     * prevent further resize recommendations to the tier family.
     */
    private boolean shouldHandleFailure(ActionFailure actionFailure) {
        if (actionFailure == null) {
            return false;
        }
        String failureMessage = actionFailure.getErrorDescription();
        return failedActionPatterns.stream()
                .anyMatch(pattern -> pattern.matcher(failureMessage).find());
    }

    /**
     * Helper function to query the repository for entities.
     *
     * @param entityOid if present, the specific OID to search for, else return all entities of
     *              the specified type.
     * @return the list of requested entities, or an empty list if none found.
     */
    private @Nonnull
    List<TopologyEntityDTO> getVMs(Long entityOid) {
        Builder builder = RetrieveTopologyEntitiesRequest.newBuilder()
                .setReturnType(Type.FULL)
                .addEntityType(EntityType.VIRTUAL_MACHINE.getValue());
        if (entityOid != null) {
            builder.addEntityOids(entityOid);
        }
        return RepositoryDTOUtil.topologyEntityStream(
                repositoryService.retrieveTopologyEntities(builder.build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
    }

    private Optional<Long> getAccountId(@Nonnull TopologyEntityDTO vm) {
        // Ensure that the VM has account information associated with this VM
        if (vm.hasOrigin() && vm.getOrigin().hasDiscoveryOrigin()) {
            Map<Long, PerTargetEntityInformation> discoveryDataMap =
                    vm.getOrigin().getDiscoveryOrigin().getDiscoveredTargetDataMap();
            if (!discoveryDataMap.isEmpty()) {
                // Get a target identifier, which is the account ID for cloud VMs.
                Map.Entry<Long, PerTargetEntityInformation> entry =
                        discoveryDataMap.entrySet().iterator().next();
                return Optional.of(entry.getKey());
            }
        }
        logger.error("Cannot find account ID in VM {}", vm.getDisplayName());
        return Optional.empty();
    }

    @VisibleForTesting
    public List<Pattern> getFailedActionPatterns() {
        return failedActionPatterns;
    }
}
