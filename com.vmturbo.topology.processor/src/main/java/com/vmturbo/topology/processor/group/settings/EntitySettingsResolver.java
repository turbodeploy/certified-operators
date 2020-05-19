package com.vmturbo.topology.processor.group.settings;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.Context;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.SettingsChunk;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.ResolvedGroup;

/**
 * Responsible for resolving the Entities -> Settings mapping as well as
 * applying settings which transform the entities in the topology.
 * One example where the Settings application would lead to transformation is
 * derived calculations:
 *     Sometimes a probe specifies a formula for calculating capacity
 *     e.g. value * multiply_by_factor(Memory_Provisioned_Factor,
 *     CPU_Provisioned_Factor, Storage_Provisoned_Factor).
 *     The actual settings values for the multiply_by_factor has to be applied
 *     by the TP.
 *
 */
public class EntitySettingsResolver {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private final GroupServiceBlockingStub groupServiceClient;

    private final SettingServiceBlockingStub settingServiceClient;

    private final SettingPolicyServiceStub settingPolicyServiceAsyncStub;

    private final ScheduleServiceBlockingStub scheduleServiceBlockingStub;

    private final int chunkSize;

    // settingSpecName -> SettingResolver
    // If settingSpecName is not present in map, we use the defaultSettingResolver.
    private final Map<String, SettingResolver> settingSpecNameToSettingResolver;

    /**
     * Create a new settings manager.
     * @param settingPolicyServiceClient The service to use to retrieve setting policy definitions.
     * @param groupServiceClient The service to use to retrieve group definitions.
     * @param settingServiceClient The service to use to retrieve setting service definitions.
     * @param settingPolicyServiceAsyncStub The service to use to retrieve setting service
*                                      definitions asynchronously.
     * @param scheduleServiceBlockingStub The service to use to retrieve schedules.
     * @param chunkSize Size of chunks for uploading entity settings to the group component.
     */
    public EntitySettingsResolver(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceClient,
                                  @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                  @Nonnull final SettingServiceBlockingStub settingServiceClient,
                                  @Nonnull final SettingPolicyServiceStub settingPolicyServiceAsyncStub,
                                  @Nonnull final ScheduleServiceBlockingStub scheduleServiceBlockingStub,
                                  final int chunkSize) {
        this.settingPolicyServiceClient = Objects.requireNonNull(settingPolicyServiceClient);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
        this.settingPolicyServiceAsyncStub = Objects.requireNonNull(settingPolicyServiceAsyncStub);
        this.scheduleServiceBlockingStub = Objects.requireNonNull(scheduleServiceBlockingStub);
        this.chunkSize = chunkSize;
        this.settingSpecNameToSettingResolver =
                ImmutableMap.of(EntitySettingSpecs.ExcludedTemplates.getSettingName(),
                        SettingResolver.tiebreakerSettingResolver);
    }

    /**
     * Resolve the groups associated with the SettingPolicies and associate the
     * entities with their settings.
     *
     * <p>Do conflict resolution when an Entity has the same Setting from
     * different SettingPolicies.
     *
     * @param groupResolver Group resolver to resolve the groups associated with the settings.
     * @param topologyGraph The topology graph on which to do the search.
     * @param settingOverrides These overrides get applied after regular setting resolution
     *                         (including conflict resolution), so all entities that have these settings
     *                         will have the requested values. For example, if "move" is overriden
     *                         to "DISABLED" then all entities that "move" applies to (e.g. VMs)
     *                         will have the "move" settings as "DISABLED" no matter what the
     *                         setting policies say. There is currently no scope to the overrides,
     *                         so all overrides are global.
     * @param topologyInfo used to get the topology context id
     * @param consistentScalingManager consistenet scaling manager
     * @return List of EntitySettings
     *
     */
    public GraphWithSettings resolveSettings(
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final SettingOverrides settingOverrides,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final ConsistentScalingManager consistentScalingManager) {

        // map from policy ID to settings policy
        final Map<Long, SettingPolicy> policyById =
            getAllSettingPolicies(settingPolicyServiceClient, topologyInfo.getTopologyContextId());

        final List<SettingPolicy> userAndDiscoveredSettingPolicies =
            SettingDTOUtil.extractUserAndDiscoveredSettingPolicies(policyById.values());

        final Set<Long> referencedGroups = userAndDiscoveredSettingPolicies.stream()
            .flatMap(sp -> sp.getInfo().getScope().getGroupsList().stream())
            .collect(Collectors.toSet());
        referencedGroups.addAll(settingOverrides.getInvolvedGroups());

        // Get and resolve groups.
        final Map<Long, ResolvedGroup> groups = getAndResolveGroups(referencedGroups, groupResolver, topologyGraph);

        // let the setting overrides handle any max utilization settings
        settingOverrides.resolveGroupOverrides(groups);

        // EntityId(OID) -> Map<<Settings.name, SettingAndPolicyIdRecord> mapping
        final Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName = new HashMap<>();
        // SettingSpecName -> SettingSpec
        final Map<String, SettingSpec> settingSpecNameToSettingSpecs = getAllSettingSpecs();

        // collect all schedules used by setting policies. Only user polices can have schedules
        final Map<Long, Schedule> schedules = getSchedules(userAndDiscoveredSettingPolicies,
            Instant.now());

        // Initial pass over policies map to identify scaling groups. This will pre-populate
        // the SettingAndPolicyIdRecord maps such that all members of a scaling group point
        // to the same map.  This way, all policies applied to any scaling group member will
        // be applied to all other entities in the scaling group as well.
        consistentScalingManager.addEntities(groups, topologyGraph, userAndDiscoveredSettingPolicies);

        // Now that all scaling group members have been identified, call the CSM to build the
        // scaling groups.  This will also populate userSettingsByEntityAndName with pre-merged
        // empty settings entries for all scaling group members.
        consistentScalingManager.buildScalingGroups(topologyGraph, groupResolver,
            userSettingsByEntityAndName);

        // Convert proto settings into topology processor settings
        final Map<SettingPolicy, Collection<TopologyProcessorSetting<?>>> policyToSettingsInPolicy =
                convertSettingsToTopologyProcessorSettings(userAndDiscoveredSettingPolicies);

        // For each group, apply the settings from the SettingPolicies associated with the group
        // to the resolved entities
        policyToSettingsInPolicy.forEach(
                (key, value) -> resolveAllEntitySettings(key, value, groups,
                        userSettingsByEntityAndName, settingSpecNameToSettingSpecs, schedules));

        final List<SettingPolicy> defaultSettingPolicies =
                SettingDTOUtil.extractDefaultSettingPolicies(policyById.values());
        // entityType -> SettingPolicyId mapping
        final Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType =
                SettingDTOUtil.arrangeByEntityType(defaultSettingPolicies);
        final Map<Long, SettingPolicy> defaultSettingPoliciesById = defaultSettingPolicies.stream()
            .collect(Collectors.toMap(SettingPolicy::getId, Function.identity()));

         // Add scaling group membership information by creating a setting on each member.  We want
         // to do this after the default settings are applied in order to avoid the membership
         // setting removing the default settings.
        consistentScalingManager.addScalingGroupSettings(userSettingsByEntityAndName);
        consistentScalingManager.clear();  // Clear all state that doesn't need to persist

        // We have applied all the user settings. Now traverse the graph and
        // for each entity, associate its user settings and default setting policy id.
        // Group Component will look at the user settings and for the missing
        // settings, it will use the default settings which is defined in the
        // default SP.
        final Map<Long, EntitySettings> settings = topologyGraph.entities()
            .map(topologyEntity -> createEntitySettingsMessage(topologyEntity,
                userSettingsByEntityAndName.getOrDefault(topologyEntity.getOid(), Collections.emptyMap())
                    .values(),
                defaultSettingPoliciesByEntityType,
                settingOverrides))
            .collect(Collectors.toMap(EntitySettings::getEntityOid, Function.identity()));
        return new GraphWithSettings(topologyGraph, settings, defaultSettingPoliciesById);
    }

    @Nonnull
    private Map<SettingPolicy, Collection<TopologyProcessorSetting<?>>> convertSettingsToTopologyProcessorSettings(
            @Nonnull List<SettingPolicy> userAndDiscoveredSettingPolicies) {
        final Map<SettingPolicy, Collection<TopologyProcessorSetting<?>>> resultMap =
                new HashMap<>();

        for (SettingPolicy settingPolicy : Objects.requireNonNull(
                userAndDiscoveredSettingPolicies)) {
            final List<Setting> settingsList = settingPolicy.getInfo().getSettingsList();
            final List<Setting> actionModeSettings = new ArrayList<>();
            final Map<String, Setting> executionScheduleSettings = new HashMap<>();
            final List<Setting> otherSettings = new ArrayList<>();

            // distribute settings into different groups
            for (Setting setting : settingsList) {
                final String settingName = setting.getSettingSpecName();
                if (EntitySettingSpecs.isExecutionScheduleSetting(settingName)) {
                    executionScheduleSettings.put(settingName, setting);
                } else if (EntitySettingSpecs.isActionModeSetting(settingName)) {
                    actionModeSettings.add(setting);
                } else {
                    otherSettings.add(setting);
                }
            }

            // group dependent ActionMode and ExecutionSchedule settings
            final List<List<Setting>> correspondingSettings =
                    groupDependentSettings(actionModeSettings, executionScheduleSettings);

            final List<TopologyProcessorSetting<?>> combinedSettings =
                    correspondingSettings.stream()
                    .map(TopologyProcessorSettingsConverter::toTopologyProcessorSetting)
                    .collect(Collectors.toList());

            final List<TopologyProcessorSetting<?>> singleSettings = otherSettings.stream()
                    .map(setting -> TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                            Collections.singleton(setting)))
                    .collect(Collectors.toList());

            final List<TopologyProcessorSetting<?>> topologyProcessorSettings =
                    Stream.of(combinedSettings, singleSettings)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());

            resultMap.put(settingPolicy, topologyProcessorSettings);
        }

        return resultMap;
    }

    /**
     * Combine dependent ActionMode and ExecutionSchedule setting in order to transform them into
     * {@link ActionModeExecutionScheduleTopologyProcessorSetting}.
     *
     * @param actionModeSettings actionMode setting
     * @param executionScheduleSettings executionSchedule setting
     * @return collection of combined dependent settings
     */
    @Nonnull
    private List<List<Setting>> groupDependentSettings(@Nonnull List<Setting> actionModeSettings,
            @Nonnull Map<String, Setting> executionScheduleSettings) {
        final List<List<Setting>> relatedSettings = new ArrayList<>();
        for (Setting actionModeSetting : actionModeSettings) {
            final String executionScheduleSpecName =
                    EntitySettingSpecs.getActionModeToExecutionScheduleSettings()
                            .get(actionModeSetting.getSettingSpecName());
            final Setting correspondingExecutionSchedule =
                    executionScheduleSettings.get(executionScheduleSpecName);
            if (correspondingExecutionSchedule != null) {
                relatedSettings.add(
                        Arrays.asList(actionModeSetting, correspondingExecutionSchedule));
            } else {
                // there is no corresponding execution schedule setting
                relatedSettings.add(Collections.singletonList(actionModeSetting));
            }
        }
        return relatedSettings;
    }

    /**
     * Resolve settings for entities in a group. If the settings use the same spec and have
     * different values, select a winner. If one setting is discovered and one is user-defined,
     * user-defined takes priority. If one setting has a scheduled period of
     * activity and another does not, the one with a schedule takes priority.
     * Otherwise use the spec's tie-breaker to resolve.
     *
     * @param settingPolicy The setting policy to apply.
     * @param settingsInPolicy List of settings from policy in topology processor representation
     * @param resolvedGroups Resolved groups by ID.
     * @param userSettingsByEntityAndName The return parameter which
     *             maps an entityId with its associated settings indexed by the
     *             settingsSpecName
     * @param settingSpecNameToSettingSpecs Map of SettingSpecName to SettingSpecs
     * @param schedules Schedules used by settings policies
     */
    @VisibleForTesting
    void resolveAllEntitySettings(@Nonnull final SettingPolicy settingPolicy,
                                  @Nonnull final Collection<TopologyProcessorSetting<?>> settingsInPolicy,
                                  @Nonnull final Map<Long, ResolvedGroup> resolvedGroups,
                                  @Nonnull Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName,
                                  @Nonnull final Map<String, SettingSpec> settingSpecNameToSettingSpecs,
                                  @Nonnull final Map<Long, Schedule> schedules) {
        checkNotNull(userSettingsByEntityAndName);

        if (inEffectNow(settingPolicy, schedules)) {
            final SettingPolicy.Type spType = settingPolicy.getSettingPolicyType();
            final Set<Long> entities = new HashSet<>();
            settingPolicy.getInfo().getScope().getGroupsList()
                .forEach(groupId -> {
                    final ResolvedGroup resolvedGroup = resolvedGroups.get(groupId);
                    if (resolvedGroup == null) {
                        logger.error("Group {} referenced by policy {} ({}) is unresolved. Skipping.",
                            groupId, settingPolicy.getId(), settingPolicy.getInfo().getName());
                    } else {
                        // Collecting into a set to de-dupe across groups in scope.
                        entities.addAll(resolvedGroup.getEntitiesOfType(ApiEntityType.fromType(settingPolicy.getInfo().getEntityType())));
                    }
                });

            for (Long oid : entities) {
                // settingSpecName-> Setting mapping. userSettingsByEntityAndName has already been
                // populated by the consistent scaling manager with shared settings maps for all
                // scaling group members.  By using a shared map, adding setting for a scaling group
                // member automatically adds it for the rest of them.
                Map<String, SettingAndPolicyIdRecord> settingsByName =
                    userSettingsByEntityAndName.computeIfAbsent(oid, key -> new HashMap<>());

                settingsInPolicy.forEach(nextSetting -> {
                    final String specName = nextSetting.getSettingSpecName();
                    final long nextSettingPolicyId = settingPolicy.getId();
                    final boolean hasSchedule = settingPolicy.getInfo().hasScheduleId();
                    if (!settingsByName.containsKey(specName)) {
                        settingsByName.put(specName, new SettingAndPolicyIdRecord(
                            nextSetting, nextSettingPolicyId, spType, hasSchedule));
                    } else {
                        // Use the corresponding resolver to resolve settings.
                        // If no resolver is associated with the specName, use the default resolver.
                        settingSpecNameToSettingResolver
                            .getOrDefault(specName, SettingResolver.defaultSettingResolver)
                            .resolve(settingsByName.get(specName), nextSetting, nextSettingPolicyId,
                                hasSchedule, spType, settingSpecNameToSettingSpecs);
                    }
                });
            }
        }
    }

    /**
     * Policy is in effect now if it doesn't have a schedule (in which case it is always in
     * effect) or if it has a schedule and the schedule applies now.
     *
     * @param sp a setting policy with or without a schedule
     * @param schedules a map of schedules used by setting policies
     * @return whether the policy is in effect
     */
    private boolean inEffectNow(@Nonnull final SettingPolicy sp,
                                @Nonnull final Map<Long, Schedule> schedules) {
        if (!sp.getInfo().hasScheduleId()) {
            return true;
        }
        final long scheduleId = sp.getInfo().getScheduleId();
        if (!schedules.containsKey(scheduleId)) {
            logger.error("Unexpectedly schedule ID {} not found in list if schedules used by " +
                "setting policy {}", () -> scheduleId, () -> sp.getId());
        } else {
            return schedules.get(scheduleId).hasActive();
        }
        return false;
    }


    /**
     * Create EntitySettings message.
     *
     * @param entity {@link TopologyEntity} whose settings should be created.
     * @param userSettings List of user Setting
     * @param defaultSettingPoliciesByEntityType Mapping of entityType to SettingPolicyId
     * @param settingOverrides The map of overrides, by setting name.
     * @return EntitySettings message
     *
     */
    private EntitySettings createEntitySettingsMessage(TopologyEntity entity,
                @Nonnull final Collection<SettingAndPolicyIdRecord> userSettings,
                @Nonnull final Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType,
                @Nonnull final SettingOverrides settingOverrides) {

        final EntitySettings.Builder entitySettingsBuilder =
                EntitySettings.newBuilder().setEntityOid(entity.getOid());

        // transform topology processor settings into protobuf settings
        userSettings.forEach(settingRecord -> {
            List<SettingToPolicyId> collect =
                    TopologyProcessorSettingsConverter.toProtoSettings(settingRecord.getSetting())
                            .stream()
                            .map(setting -> SettingToPolicyId.newBuilder()
                                    .setSetting(setting)
                                    .addAllSettingPolicyId(settingRecord.getSettingPolicyIdList())
                                    .build())
                            .collect(Collectors.toList());
            entitySettingsBuilder.addAllUserSettings(collect);
        });

        // Override user settings.
        settingOverrides.overrideSettings(entity.getTopologyEntityDtoBuilder(), entitySettingsBuilder);

        if (defaultSettingPoliciesByEntityType.containsKey(entity.getEntityType())) {
            entitySettingsBuilder.setDefaultSettingPolicyId(
                defaultSettingPoliciesByEntityType.get(entity.getEntityType()).getId());
        }

        return entitySettingsBuilder.build();
    }

    /**
     * Send entitySettings mapping to the Group component.
     *
     * @param topologyInfo The information about the topology which was used to resolve
     *                    the settings.
     * @param entitiesSettings List of EntitySettings messages
     * @param graph The topology graph
     */
    public void sendEntitySettings(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final Collection<EntitySettings> entitiesSettings,
                                   @Nonnull final TopologyGraph<TopologyEntity> graph) {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        // For now, don't upload settings for non-realtime topologies.
        StreamObserver<UploadEntitySettingsResponse> responseObserver =
            new StreamObserver<UploadEntitySettingsResponse>() {

                @Override
                public void onNext(final UploadEntitySettingsResponse value) {

                }

                @Override
                public void onError(final Throwable t) {
                    Status status = Status.fromThrowable(t);
                    logger.error("Failed to upload EntitySettings map to group component"
                        + " for topology {}, due to {}", topologyInfo, status);
                    finishLatch.countDown();

                }

                @Override
                public void onCompleted() {
                    logger.warn("Finished uploading EntitySettings map to group component"
                        + " for topology {}.", topologyInfo);
                    finishLatch.countDown();
                }
            };

        StreamObserver<UploadEntitySettingsRequest> requestObserver =
                settingPolicyServiceAsyncStub.uploadEntitySettings(responseObserver);
        try {
            if (!TopologyDTOUtil.isPlan(topologyInfo)) {
                streamEntitySettingsRequest(topologyInfo, entitiesSettings, requestObserver);
            } else {
                // Topology is a plan topology. Save the settings in the group component.
                // Only send settings for VMs to the group component.
                List<EntitySettings> vmSettings = entitiesSettings.stream().filter(e -> {
                    TopologyEntity entity = graph.getEntity(e.getEntityOid()).orElse(null);
                    return entity != null &&
                            entity.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue();
                }).collect(Collectors.toList());
                streamEntitySettingsRequest(topologyInfo, vmSettings, requestObserver);
            }
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
        try {
            // block until we get a response or an exception occurs.
            finishLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // set interrupt flag
            logger.error("Interrupted while waiting for response", e);
        }
    }

    /**
     * Stream an EntitySettingsRequest over a StreamObserver.
     *
     * @param topologyInfo The information about the topology which was used to resolve
     *                    the settings.
     * @param entitiesSettings List of EntitySettings messages
     *
     * @param requestObserver Stream observer on which to write the entity settings
     */
    @VisibleForTesting
    void streamEntitySettingsRequest(@Nonnull final TopologyInfo topologyInfo,
                              @Nonnull final Collection<EntitySettings> entitiesSettings,
                              @Nonnull final StreamObserver<UploadEntitySettingsRequest> requestObserver) {
        requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
            .setContext(Context.newBuilder()
                .setTopologyContextId(topologyInfo.getTopologyContextId())
                .setTopologyId(topologyInfo.getTopologyId()))
            .build());
        Iterators.partition(entitiesSettings.iterator(), chunkSize)
            .forEachRemaining(chunk -> {
                requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
                    .setSettingsChunk(SettingsChunk.newBuilder()
                        .addAllEntitySettings(chunk))
                    .build());
            });
    }

    /**
     * Get all SettingPolicies from Group Component (GC).
     *
     * @param settingPolicyServiceClient Client for communicating with SettingPolicyService
     * @param contextId the topology context ID (used in the response message)
     * @return List of Setting policies.
     *
     */
    private Map<Long, SettingPolicy> getAllSettingPolicies(
            SettingPolicyServiceBlockingStub settingPolicyServiceClient, long contextId) {

        final List<SettingPolicy> settingPolicies = new LinkedList<>();
        settingPolicyServiceClient.listSettingPolicies(
                ListSettingPoliciesRequest.newBuilder()
                       .setContextId(contextId)
                       .build())
                       .forEachRemaining(settingPolicies::add);

        return settingPolicies.stream()
                        .collect(Collectors.toMap(SettingPolicy::getId, Function.identity()));
    }

    /**
     * Get all SettingSpecs from Group Component (GC).
     *
     * @return Map of SettingSpecName to SettingSpec
     *
     */
    private Map<String, SettingSpec> getAllSettingSpecs() {

        Map<String, SettingSpec> settingNameToSettingSpecs = new HashMap<>();

        settingServiceClient.searchSettingSpecs(
            SearchSettingSpecsRequest.getDefaultInstance())
                .forEachRemaining(spec -> {
                    if (spec.hasName()) {
                        // SettingSpec name should be unique.
                        // Will assume that GC has already done the validation
                        settingNameToSettingSpecs.put(spec.getName(), spec);
                    } else {
                        logger.warn("settingSpec has missing name: {}", spec);
                    }
                });

        return settingNameToSettingSpecs;
    }

    private Map<Long, ResolvedGroup> getAndResolveGroups(
            @Nonnull final Set<Long> groupIds,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> graph) {
        final Map<Long, ResolvedGroup> groups = new HashMap<>();

        if (groupIds.isEmpty()) {
            return groups;
        }

        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addAllId(groupIds))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())
            .forEachRemaining(group -> {
                if (group.hasId()) {
                    try {
                        groups.put(group.getId(), groupResolver.resolve(group, graph));
                    } catch (GroupResolutionException e) {
                        // Continue trying to resolve other groups.
                        logger.error("Failed to resolve group " + group.getId(), e);
                    }
                } else {
                    logger.warn("Group has no id. Skipping. {}", group);
                }
            });

        return groups;
    }

    /**
     * Get all schedules used by settings policies upfront.
     *
     * @param settingPolicies Setting policies to resolve
     * @param resolutionInstant Instant to resolve sschedule
     * @return Map of {@link Schedule} keyed by schedule ID
     */
    @Nonnull
    private Map<Long, Schedule> getSchedules(@Nonnull final List<SettingPolicy> settingPolicies,
                                             @Nonnull  final Instant resolutionInstant) {
        List<Long> scheduleIds = settingPolicies.stream().filter(sPolicy ->
            sPolicy.getInfo().hasScheduleId())
            .map(settingPolicy -> settingPolicy.getInfo().getScheduleId())
            .collect(Collectors.toList());
        final Map<Long, Schedule> scheduleMap = Maps.newHashMap();
        if (!scheduleIds.isEmpty()) {
            scheduleServiceBlockingStub.getSchedules(GetSchedulesRequest.newBuilder()
                .addAllOid(scheduleIds)
                .setRefTime(resolutionInstant.toEpochMilli())
                .build())
                .forEachRemaining(schedule -> scheduleMap.put(schedule.getId(), schedule));
        }
        return scheduleMap;
    }

    /**
     * Helper class to store the setting and the setting policy ID it is associated with.
     */
    @VisibleForTesting
    public static class SettingAndPolicyIdRecord {

        private TopologyProcessorSetting<?> setting;
        private Set<Long> settingPolicyIdList;
        private SettingPolicy.Type type;
        private boolean scheduled;

        /**
         * Constructor of {@link SettingAndPolicyIdRecord}.
         *
         * @param setting topologyProcessor setting value
         * @param settingPolicyId setting policy id
         * @param type type of setting policy
         * @param scheduled define is scheduled setting policy or not
         */
        public SettingAndPolicyIdRecord(@Nonnull final TopologyProcessorSetting<?> setting,
                final long settingPolicyId, @Nonnull final SettingPolicy.Type type,
                final boolean scheduled) {
            set(setting, Collections.singleton(settingPolicyId), type, scheduled);
        }

        void set(final TopologyProcessorSetting<?> setting, Collection<Long> settingPolicyIds,
                SettingPolicy.Type type, boolean scheduled) {
            this.setting = setting;
            this.settingPolicyIdList = new HashSet<>(1);
            this.settingPolicyIdList.addAll(settingPolicyIds);
            this.type = type;
            this.scheduled = scheduled;
        }

        TopologyProcessorSetting<?> getSetting() {
            return setting;
        }

        Set<Long> getSettingPolicyIdList() {
            return settingPolicyIdList;
        }

        SettingPolicy.Type getType() {
            return type;
        }

        boolean isScheduled() {
            return scheduled;
        }
    }

    /**
     * This functional interface defines a setting resolver.
     */
    @FunctionalInterface
    interface SettingResolver {
        /**
         * Resolve two settings.
         *
         * @param existingRecord existing resolved record
         * @param nextSetting next setting
         * @param nextSettingPolicyId the settingPolicyId the next setting belongs to
         * @param nextIsScheduled if the next setting is scheduled
         * @param nextType the type of the next setting
         * @param settingSpecNameToSettingSpecs mapping from settingSpecName to associated SettingSpec
         * @return the resolved {@link SettingAndPolicyIdRecord}
         */
        Optional<SettingAndPolicyIdRecord> resolve(
            SettingAndPolicyIdRecord existingRecord,
            TopologyProcessorSetting<?> nextSetting,
            long nextSettingPolicyId, boolean nextIsScheduled,
            SettingPolicy.Type nextType,
            Map<String, SettingSpec> settingSpecNameToSettingSpecs);

        default SettingResolver thenResolve(@Nonnull final SettingResolver other) {
            return (existingRecord, nextSetting, nextSettingPolicyId,
                    nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
                Optional<SettingAndPolicyIdRecord> result = this.resolve(
                    existingRecord, nextSetting, nextSettingPolicyId,
                    nextIsScheduled, nextType, settingSpecNameToSettingSpecs);
                return result.isPresent() ? result : other.resolve(
                    existingRecord, nextSetting, nextSettingPolicyId,
                    nextIsScheduled, nextType, settingSpecNameToSettingSpecs);
            };
        }

        /* ---------------- Static Fields -------------- */

        /**
         * A USER setting wins over DISCOVERED setting.
         */
        SettingResolver userWinOverDiscoveredSettingResolver =
                (existingRecord, nextSetting, nextSettingPolicyId,
                 nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
            final SettingPolicy.Type existingType = existingRecord.getType();
            if (existingType == nextType) {
                return Optional.empty();
            } else if (existingType == SettingPolicy.Type.DISCOVERED &&
                nextType == SettingPolicy.Type.USER) {
                existingRecord.set(nextSetting, Collections.singleton(nextSettingPolicyId), nextType, nextIsScheduled);
            }
            return Optional.of(existingRecord);
        };

        /**
         * A scheduled setting wins over a non-scheduled setting.
         */
        SettingResolver scheduledWinOverNonScheduledSettingResolver =
                (existingRecord, nextSetting, nextSettingPolicyId,
                 nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
            final boolean existingSettingHasSchedule = existingRecord.isScheduled();
            if ((existingSettingHasSchedule && nextIsScheduled) ||
                (!existingSettingHasSchedule && !nextIsScheduled)) {
                return Optional.empty();
            } else if (!existingSettingHasSchedule) {
                existingRecord.set(nextSetting, Collections.singleton(nextSettingPolicyId), nextType, true);
            }
            return Optional.of(existingRecord);
        };

        /**
         * Use tie breaker to resolve settings.
         */
        SettingResolver tiebreakerSettingResolver =
                (existingRecord, nextSetting, nextSettingPolicyId,
                 nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
            final Pair<TopologyProcessorSetting<?>, Boolean>
                    resolvedPair = applyTiebreaker(nextSetting, existingRecord.getSetting(),
                            settingSpecNameToSettingSpecs);
            final TopologyProcessorSetting<?> winnerSetting = resolvedPair.getFirst();
            if (winnerSetting != existingRecord.getSetting()) {
                final Boolean isMergedSettingValuesFromSeveralPolicies = resolvedPair.getSecond();
                final Set<Long> associatedPolicies = new HashSet<>();
                associatedPolicies.add(nextSettingPolicyId);
                // if winner setting merged values from several settings than we save link to all
                // policies
                if (isMergedSettingValuesFromSeveralPolicies) {
                    associatedPolicies.addAll(existingRecord.getSettingPolicyIdList());
                }
                existingRecord.set(winnerSetting, associatedPolicies, nextType, nextIsScheduled);
            }
            return Optional.of(existingRecord);
        };

        /**
         * Determine which of an existing setting and a new setting, with the same spec name but
         * different values, should apply to an entity. If one is USER defined and one is DISCOVERED
         * then the USER defined policy wins. If one has a schedule and the other doesn't, the one
         * with a schedule wins. If both have a schedule or both don't, their spec's tie-breaker
         * is used to resolve the conflict.
         */
        SettingResolver defaultSettingResolver =
            userWinOverDiscoveredSettingResolver
                .thenResolve(scheduledWinOverNonScheduledSettingResolver)
                .thenResolve(tiebreakerSettingResolver);

        /* ---------------- Static utilities -------------- */

        /**
         * Resolve conflict when 2 settings have the same spec but
         * different values.
         *
         * <p>The tie-breaker to resolve conflict is defined in the SettingSpec.
         *
         * @param newSetting Setting message
         * @param existingSetting Setting message
         * @param settingNameToSettingSpecs Mapping from SettingSpecName to SettingSpec
         * @return Resolved setting which won the tieBreaker
         */
        static Pair<TopologyProcessorSetting<?>, Boolean> applyTiebreaker(
                @Nonnull final TopologyProcessorSetting<?> newSetting,
                @Nonnull final TopologyProcessorSetting<?> existingSetting,
                @Nonnull final Map<String, SettingSpec> settingNameToSettingSpecs) {
            SettingResolver.commonChecks(newSetting, existingSetting, settingNameToSettingSpecs);
            String specName = newSetting.getSettingSpecName();
            SettingSpec settingSpec = settingNameToSettingSpecs.get(specName);

            // Verified above that both settings are of same type. Hence they should
            // both have the same tie-breaker. So just extract it from one of the setting.
            final SettingTiebreaker tieBreaker = settingSpec.getEntitySettingSpec().getTiebreaker();
            return defineWinnerSetting(newSetting, existingSetting, settingSpec,
                    tieBreaker);
        }

        /**
         * Compare two setting values and define winner depend on tiebreaker value.
         *
         * <p>No validation is done in this method. Assumes all the
         * input types and values are correct.
         *
         * @param newSetting Topology processor setting representation
         * @param existingSetting Topology processor setting representation
         * @param settingSpec SettingSpec definition referred by the setting
         *                     messages. Both input settings should have the same
         *                     settingSpec name
         * @param tieBreaker tiebreaker value
         *
         * @return winner setting
         *
         */
        static Pair<TopologyProcessorSetting<?>, Boolean> defineWinnerSetting(
                @Nonnull final TopologyProcessorSetting newSetting,
                @Nonnull final TopologyProcessorSetting existingSetting,
                @Nonnull final SettingSpec settingSpec, @Nonnull SettingTiebreaker tieBreaker) {
            return existingSetting.resolveConflict(newSetting, tieBreaker, settingSpec);
        }

        /**
         * Check before we resolve two settings.
         *
         * @param setting1 one topology processor setting
         * @param setting2 the other topology processor setting
         * @param settingSpecNameToSettingSpecs mapping from settingSpecName to associated SettingSpec
         */
        static void commonChecks(@Nonnull final TopologyProcessorSetting<?> setting1,
                @Nonnull final TopologyProcessorSetting<?> setting2,
                @Nonnull final Map<String, SettingSpec> settingSpecNameToSettingSpecs) {
            Preconditions.checkArgument(!settingSpecNameToSettingSpecs.isEmpty(), "Empty setting specs");

            String specName1 = setting1.getSettingSpecName();
            String specName2 = setting2.getSettingSpecName();

            Preconditions.checkArgument(specName1.equals(specName2),
                "Settings have different spec names");
            Preconditions.checkArgument(settingSpecNameToSettingSpecs.get(specName1).hasEntitySettingSpec(),
                "SettingSpec should be of type EntitySettingSpec");
            Preconditions.checkArgument(settingSpecNameToSettingSpecs.get(specName2).hasEntitySettingSpec(),
                "SettingSpec should be of type EntitySettingSpec");
            Preconditions.checkArgument(setting1.getClass().equals(setting2.getClass()),
                    "Compared topology processor settings should be represented as objects of the same class");
        }
    }
}
