package com.vmturbo.topology.processor.group.settings;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.common.protobuf.ListUtil.mergeTwoSortedListsAndRemoveDuplicates;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
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

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.Context;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.SettingsChunk;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

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
     *  @param settingPolicyServiceClient The service to use to retrieve setting policy definitions.
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
        this.settingSpecNameToSettingResolver = ImmutableMap.of(
            EntitySettingSpecs.ExcludedTemplates.getSettingName(),
            SettingResolver.sortedSetOfOidUnionSettingResolver
        );
    }

    /**
     * Resolve the groups associated with the SettingPolicies and associate the
     * entities with their settings.
     *
     * Do conflict resolution when an Entity has the same Setting from
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

        // groupId -> SettingPolicies mapping
        final Map<Long, List<SettingPolicy>> groupSettingPoliciesMap =
            getGroupSettingPolicyMapping(userAndDiscoveredSettingPolicies);

        // groups used for applying entity/group-specific settings
        final Set<Long> overrideGroups = settingOverrides.getInvolvedGroups();

        final Map<Long, Grouping> groups =
            getGroupInfo(groupServiceClient, Sets.union(groupSettingPoliciesMap.keySet(), overrideGroups));

        // let the setting overrides handle any max utilization settings
        settingOverrides.resolveGroupOverrides(groups, groupResolver, topologyGraph);

        // EntityId(OID) -> Map<<Settings.name, SettingAndPolicyIdRecord> mapping
        final Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName = new HashMap<>();
        // SettingSpecName -> SettingSpec
        final Map<String, SettingSpec> settingSpecNameToSettingSpecs = getAllSettingSpecs();

        // collect all schedules used by setting policies. Only user polices can have schedules
        final Map<Long, Schedule> schedules = getSchedules(userAndDiscoveredSettingPolicies,
            Instant.now());

        // For each group, resolve it to get its entities. Then apply the settings
        // from the SettingPolicies associated with the group to the resolved entities
        groupSettingPoliciesMap.forEach((groupId, settingPolicies) -> {
            // This may be inefficient as we are walking the graph every time to resolve a group.
            // Resolving a bunch of groups at once(ORing of the group search parameters)
            // would probably be more efficient.
            try {
                final Grouping group = groups.get(groupId);
                if (group != null ) {
                    final Set<Long> allEntitiesInGroup = groupResolver.resolve(group, topologyGraph);
                    consistentScalingManager.addEntities(group,
                        topologyGraph.getEntities(allEntitiesInGroup), settingPolicies);
                    resolveAllEntitySettings(allEntitiesInGroup, settingPolicies,
                        userSettingsByEntityAndName, settingSpecNameToSettingSpecs, schedules);
                } else {
                    logger.error("Group {} does not exist.", groupId);
                }
            } catch (GroupResolutionException gre) {
                // should we throw an exception ?
                logger.error("Failed to resolve group with id: {}", groupId, gre);
            }
        });

        // Now that all scaling group members have been identified, call the CSM to build the
        // scaling groups.
        consistentScalingManager.buildScalingGroups(topologyGraph, groupResolver);
        // For each scaling group, apply all of the policies discovered in the group.  This will
        // not only merge template exclusions, but also all all other settings.  The tiebreaker for
        // each setting will determine which setting will be used for the entire scaling group.
        consistentScalingManager.getPoliciesStream()
            .forEach(p -> resolveAllEntitySettings(p.first, p.second,
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

    /**
     * Resolve settings for a set of entities. If the settings use the same spec and have
     * different values, select a winner. If one setting is discovered and one is user-defined,
     * user-defined takes priority. If one setting has a scheduled period of
     * activity and another does not, the one with a schedule takes priority.
     * Otherwise use the spec's tie-breaker to resolve.
     *
     * @param entities List of entity OIDs
     * @param settingPolicies List of settings policies to be applied to the entities
     * @param userSettingsByEntityAndName The return parameter which
     *             maps an entityId with its associated settings indexed by the
     *             settingsSpecName
     * @param settingSpecNameToSettingSpecs Map of SettingSpecName to SettingSpecs
     * @param schedules Schedules used by settings policies
     */
    @VisibleForTesting
    void resolveAllEntitySettings(@Nonnull final Set<Long> entities,
                                  @Nonnull final List<SettingPolicy> settingPolicies,
                                  @Nonnull Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName,
                                  @Nonnull final Map<String, SettingSpec> settingSpecNameToSettingSpecs,
                                  @Nonnull final Map<Long, Schedule> schedules) {
        checkNotNull(userSettingsByEntityAndName);

        for (Long oid: entities) {
            // settingSpecName-> Setting mapping
            Map<String, SettingAndPolicyIdRecord> settingsByName =
                userSettingsByEntityAndName.computeIfAbsent(oid, key -> new HashMap<>());

            for (SettingPolicy sp : settingPolicies) {
                if (inEffectNow(sp, schedules)) {
                    SettingPolicy.Type nextType = sp.getSettingPolicyType();
                    sp.getInfo().getSettingsList().forEach(nextSetting -> {
                        final String specName = nextSetting.getSettingSpecName();
                        final long nextSettingPolicyId = sp.getId();
                        final boolean hasSchedule = sp.getInfo().hasScheduleId();
                        if (!settingsByName.containsKey(specName)) {
                            settingsByName.put(specName, new SettingAndPolicyIdRecord(
                                nextSetting, nextSettingPolicyId, nextType, hasSchedule));
                        } else {
                            // Use the corresponding resolver to resolve settings.
                            // If no resolver is associated with the specName, use the default resolver.
                            settingSpecNameToSettingResolver
                                .getOrDefault(specName, SettingResolver.defaultSettingResolver)
                                .resolve(settingsByName.get(specName), nextSetting, nextSettingPolicyId,
                                    hasSchedule, nextType, settingSpecNameToSettingSpecs);
                        }
                    });
                }
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
            logger.error("Unexpectedly scheule ID {} not found in list if schedules used by " +
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
            EntitySettings.newBuilder()
                .setEntityOid(entity.getOid());
        userSettings.forEach(settingRecord ->
            entitySettingsBuilder.addUserSettings(
                EntitySettings.SettingToPolicyId.newBuilder()
                    .setSetting(settingRecord.getSetting())
                    .addAllSettingPolicyId(settingRecord.getSettingPolicyIdList())
                    .build())
        );

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
     */
    public void sendEntitySettings(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final Collection<EntitySettings> entitiesSettings) {
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

        if (topologyInfo.getTopologyType().equals(TopologyType.REALTIME)) {
            StreamObserver<UploadEntitySettingsRequest> requestObserver =
                    settingPolicyServiceAsyncStub.uploadEntitySettings(responseObserver);
            try {
                streamEntitySettingsRequest(topologyInfo, entitiesSettings, requestObserver);
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

    /**
     * Extract the groups which are part of the SettingPolicies and return a mapping
     * from the GroupId to the list of setting policies associated with the
     * group.
     *
     * @param settingPolicies List of SettingPolicy
     * @return Mapping of the groupId to SettingPolicy
     *
     */
    private Map<Long, List<SettingPolicy>> getGroupSettingPolicyMapping(
        List<SettingPolicy> settingPolicies) {

        Map<Long, List<SettingPolicy>> groupSettingPoliciesMap = new HashMap<>();
        settingPolicies.forEach((settingPolicy) -> {
            settingPolicy.getInfo().getScope().getGroupsList()
                .forEach((groupId) -> {
                    groupSettingPoliciesMap.computeIfAbsent(
                    groupId, k -> new LinkedList<>()).add(settingPolicy);
                });
        });
        return groupSettingPoliciesMap;
    }

    /**
     * Query the GroupInfo from GroupComponent for the provided GroupIds.
     *
     * @param groupServiceClient Client for communicating with Group Service
     * @param groupIds List of groupIds whose Group definitions has to be fetched
     * @return Map of groupId and its Group object
     */
    private Map<Long, Grouping> getGroupInfo(GroupServiceBlockingStub groupServiceClient,
                                          Collection<Long> groupIds) {

        final Map<Long, Grouping> groups = new HashMap<>();

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
                    groups.put(group.getId(), group);
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

        private Setting setting;
        private Set<Long> settingPolicyIdList;
        private SettingPolicy.Type type;
        private boolean scheduled;

        public SettingAndPolicyIdRecord(@Nonnull final Setting setting, final long settingPolicyId,
                                 @Nonnull final SettingPolicy.Type type, final boolean scheduled) {
            set(setting, settingPolicyId, type, scheduled);
        }

        void set(final Setting setting, long settingPolicyId, SettingPolicy.Type type, boolean scheduled) {
            this.setting = setting;
            this.settingPolicyIdList = new HashSet<>(1);
            this.settingPolicyIdList.add(settingPolicyId);
            this.type = type;
            this.scheduled = scheduled;
        }

        Setting getSetting() {
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

        void setSetting(final Setting setting) {
            this.setting = setting;
        }

        void addSettingPolicyId(final long settingPolicyId) {
            this.settingPolicyIdList.add(settingPolicyId);
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
            Setting nextSetting,
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
                existingRecord.set(nextSetting, nextSettingPolicyId, nextType, nextIsScheduled);
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
                existingRecord.set(nextSetting, nextSettingPolicyId, nextType, true);
            }
            return Optional.of(existingRecord);
        };

        /**
         * Use tie breaker to resolve settings.
         */
        SettingResolver tiebreakerSettingResolver =
                (existingRecord, nextSetting, nextSettingPolicyId,
                 nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
            final Setting resultSetting = applyTiebreaker(nextSetting, existingRecord.getSetting(),
                settingSpecNameToSettingSpecs);
            if (resultSetting != existingRecord.getSetting()) {
                existingRecord.set(resultSetting, nextSettingPolicyId, nextType, nextIsScheduled);
            }
            return Optional.of(existingRecord);
        };

        /**
         * This resolver merges two sorted sets of oid of two settings with UNION SettingTiebreaker.
         */
        SettingResolver sortedSetOfOidUnionSettingResolver =
                (existingRecord, nextSetting, nextSettingPolicyId,
                 nextIsScheduled, nextType, settingSpecNameToSettingSpecs) -> {
            final Setting existingSetting = existingRecord.getSetting();
            SettingResolver.commonChecks(existingSetting, nextSetting, settingSpecNameToSettingSpecs);
            Preconditions.checkArgument(
                settingSpecNameToSettingSpecs.get(existingSetting.getSettingSpecName())
                    .getEntitySettingSpec().getTiebreaker() == SettingTiebreaker.UNION,
                "Settings should have UNION SettingTiebreaker.");
            Preconditions.checkArgument(existingSetting.hasSortedSetOfOidSettingValue(),
                MessageFormat.format("Settings {0} and {1} have wrong settingValue {2}",
                    existingSetting, nextSetting, existingSetting.getValueCase()));

            // Update existing record.
            final List<Long> sortedOids1 = existingSetting.getSortedSetOfOidSettingValue().getOidsList();
            final List<Long> sortedOids2 = nextSetting.getSortedSetOfOidSettingValue().getOidsList();
            final List<Long> sortedOids = mergeTwoSortedListsAndRemoveDuplicates(sortedOids1, sortedOids2);
            existingRecord.setSetting(Setting.newBuilder()
                .setSettingSpecName(existingSetting.getSettingSpecName())
                .setSortedSetOfOidSettingValue(
                    SortedSetOfOidSettingValue.newBuilder().addAllOids(sortedOids)).build());
            // SettingPolicyIds are in a certain order because
            // the order of setting policies associated with every group is certain.
            if (nextSettingPolicyId != 0L) {
                existingRecord.addSettingPolicyId(nextSettingPolicyId);
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
         * The tie-breaker to resolve conflict is defined in the SettingSpec.
         *
         * @param setting1 Setting message
         * @param setting2 Setting message
         * @param settingNameToSettingSpecs Mapping from SettingSpecName to SettingSpec
         * @return Resolved setting which won the tieBreaker
         */
        static Setting applyTiebreaker(@Nonnull final Setting setting1, @Nonnull final Setting setting2,
                                       @Nonnull final Map<String, SettingSpec> settingNameToSettingSpecs) {
            SettingResolver.commonChecks(setting1, setting2, settingNameToSettingSpecs);
            String specName = setting1.getSettingSpecName();
            SettingSpec settingSpec = settingNameToSettingSpecs.get(specName);

            // Verified above that both settings are of same type. Hence they should
            // both have the same tie-breaker. So just extract it from one of the setting.
            SettingTiebreaker tieBreaker = settingSpec.getEntitySettingSpec().getTiebreaker();
            int ret = compareSettingValues(setting1, setting2, settingSpec);
            switch (tieBreaker) {
                case BIGGER:
                    return (ret >= 0) ? setting1 : setting2;
                case SMALLER:
                    return (ret <= 0) ? setting1 : setting2;
                default:
                    // shouldn't reach here.
                    throw new IllegalArgumentException("Illegal tiebreaker value : " + tieBreaker);
            }
        }

        /**
         * Compare two setting values.
         *
         * No validation is done in this method. Assumes all the
         * input types and values are correct.
         *
         * @param setting1 Setting message
         * @param setting2 Setting message
         * @param settingSpec SettingSpec definiton referred by the setting
         *                     messages. Both input settings should have the same
         *                     settingSpec name
         *
         * @return Positive, negative or zero integer where setting1 value is
         *         greater than, smaller than or equal to setting2 value respectively.
         *
         */
        static int compareSettingValues(@Nonnull final Setting setting1,
                                        @Nonnull final Setting setting2,
                                        @Nonnull final SettingSpec settingSpec) {
            // Maybe it's better to create special types which extend java
            // primitive type objects
            switch (setting1.getValueCase()) {
                case BOOLEAN_SETTING_VALUE:
                    return Boolean.compare(
                        setting1.getBooleanSettingValue().getValue(),
                        setting2.getBooleanSettingValue().getValue());
                case NUMERIC_SETTING_VALUE:
                    return Float.compare(
                        setting1.getNumericSettingValue().getValue(),
                        setting2.getNumericSettingValue().getValue());
                case STRING_SETTING_VALUE:
                    return setting1.getStringSettingValue().getValue()
                        .compareTo(setting2.getStringSettingValue().getValue());
                case ENUM_SETTING_VALUE:
                    return SettingDTOUtil.compareEnumSettingValues(
                        setting1.getEnumSettingValue(),
                        setting2.getEnumSettingValue(),
                        settingSpec.getEnumSettingValueType());
                default:
                    throw new IllegalArgumentException("Illegal setting value type: "
                        + setting1.getValueCase());
            }
        }

        /**
         * Check before we resolve two settings.
         *
         * @param setting1 one setting
         * @param setting2 the other setting
         * @param settingSpecNameToSettingSpecs mapping from settingSpecName to associated SettingSpec
         */
        static void commonChecks(@Nonnull final Setting setting1, @Nonnull final Setting setting2,
                                 @Nonnull final Map<String, SettingSpec> settingSpecNameToSettingSpecs) {
            Preconditions.checkArgument(!settingSpecNameToSettingSpecs.isEmpty(), "Empty setting specs");

            String specName1 = setting1.getSettingSpecName();
            String specName2 = setting2.getSettingSpecName();

            Preconditions.checkArgument(specName1.equals(specName2),
                "Settings have different spec names");
            Preconditions.checkArgument(setting1.getValueCase() == setting2.getValueCase(),
                "Settings have different value types");
            Preconditions.checkArgument(settingSpecNameToSettingSpecs.get(specName1).hasEntitySettingSpec(),
                "SettingSpec should be of type EntitySettingSpec");
            Preconditions.checkArgument(settingSpecNameToSettingSpecs.get(specName2).hasEntitySettingSpec(),
                "SettingSpec should be of type EntitySettingSpec");
        }
    }
}
