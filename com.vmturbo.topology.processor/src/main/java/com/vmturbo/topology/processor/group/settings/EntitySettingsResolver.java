package com.vmturbo.topology.processor.group.settings;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.groupingBy;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
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

    private final int chunkSize;

    /**
     * Create a new settings manager.
     *
     * @param settingPolicyServiceClient The service to use to retrieve setting policy definitions.
     * @param groupServiceClient The service to use to retrieve group definitions.
     * @param settingServiceClient The service to use to retrieve setting service definitions.
     * @param settingPolicyServiceAsyncStub The service to use to retrieve setting service
     *                                      definitions asynchronously.
     */
    public EntitySettingsResolver(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceClient,
                                  @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                  @Nonnull final SettingServiceBlockingStub settingServiceClient,
                                  @Nonnull final SettingPolicyServiceStub settingPolicyServiceAsyncStub,
                                  @Nonnull final int chunkSize) {
        this.settingPolicyServiceClient = Objects.requireNonNull(settingPolicyServiceClient);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
        this.settingPolicyServiceAsyncStub = Objects.requireNonNull(settingPolicyServiceAsyncStub);
        this.chunkSize = Objects.requireNonNull(chunkSize);
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
     * @return List of EntitySettings
     *
     */
    public GraphWithSettings resolveSettings(
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final SettingOverrides settingOverrides,
            @Nonnull final TopologyInfo topologyInfo) {

        final Map<Long, SettingPolicy> policyById =
            getAllSettingPolicies(settingPolicyServiceClient, topologyInfo.getTopologyContextId());

        final List<SettingPolicy> userAndDiscoveredSettingPolicies =
            SettingDTOUtil.extractUserAndDiscoveredSettingPolicies(policyById.values());

        // groupId -> SettingPolicies mapping
        final Map<Long, List<SettingPolicy>> groupSettingPoliciesMap =
            getGroupSettingPolicyMapping(userAndDiscoveredSettingPolicies);

        // groups used for applying entity/group-specific settings
        final Set<Long> overrideGroups = settingOverrides.getInvolvedGroups();

        final Map<Long, Group> groups =
            getGroupInfo(groupServiceClient, Sets.union(groupSettingPoliciesMap.keySet(), overrideGroups));

        // let the setting overrides handle any max utilization settings
        settingOverrides.resolveGroupOverrides(groups, groupResolver, topologyGraph);

        // EntityId(OID) -> Map<<Settings.name, SettingAndPolicyIdRecord> mapping
        final Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName = new HashMap<>();
        // SettingSpecName -> SettingSpec
        final Map<String, SettingSpec> settingNameToSettingSpecs = getAllSettingSpecs();

        // entity id -> Map<Setting name, whether setting has a schedule for entity>
        final Map<Long, Map<String, Boolean>> settingsWithSchedule = new HashMap<>();

        // resolver to determine if a schedule applies at the canonical moment (i.e. right now)
        final ScheduleResolver scheduleResolver = new ScheduleResolver(Instant.now());

        // For each group, resolve it to get its entities. Then apply the settings
        // from the SettingPolicies associated with the group to the resolved entities
        groupSettingPoliciesMap.forEach((groupId, settingPolicies) -> {
            // This may be inefficient as we are walking the graph every time to resolve a group.
            // Resolving a bunch of groups at once(ORing of the group search parameters)
            // would probably be more efficient.
            try {
                final Group group = groups.get(groupId);
                if (group != null ) {
                    final Set<Long> allEntitiesInGroup = groupResolver.resolve(group, topologyGraph);
                    resolveAllEntitySettings(allEntitiesInGroup, settingPolicies, policyById, scheduleResolver,
                            settingsWithSchedule, userSettingsByEntityAndName,
                            settingNameToSettingSpecs);
                } else {
                    logger.error("Group {} does not exist.", groupId);
                }
            } catch (GroupResolutionException gre) {
                // should we throw an exception ?
                logger.error("Failed to resolve group with id: {}", groupId, gre);
            }
        });

        final List<SettingPolicy> defaultSettingPolicies =
                SettingDTOUtil.extractDefaultSettingPolicies(policyById.values());

        // entityType -> SettingPolicyId mapping
        final Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType =
                SettingDTOUtil.arrangeByEntityType(defaultSettingPolicies);

        final Map<Long, SettingPolicy> defaultSettingPoliciesById = defaultSettingPolicies.stream()
            .collect(Collectors.toMap(SettingPolicy::getId, Function.identity()));

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
     * @param policyById map from policy ID to settings policy
     * @param scheduleResolver Used to determine if a settings policy schedule applies when
     *             the settings are being resolved.
     * @param settingsByEntityWithSchedule Used to mark whether a setting spec has a scheduled
     *             value per entity
     * @param userSettingsByEntityAndName The return parameter which
     *             maps an entityId with its associated settings indexed by the
     *             settingsSpecName
     * @param settingNameToSettingSpecs Map of SettingSpecName to SettingSpecs
     */
    @VisibleForTesting
    void resolveAllEntitySettings(final Set<Long> entities,
                                  final List<SettingPolicy> settingPolicies,
                                  final Map<Long, SettingPolicy> policyById,
                                  final ScheduleResolver scheduleResolver,
                                  Map<Long, Map<String, Boolean>> settingsByEntityWithSchedule,
                                  Map<Long, Map<String, SettingAndPolicyIdRecord>> userSettingsByEntityAndName,
                                  final Map<String, SettingSpec> settingNameToSettingSpecs) {

        checkNotNull(userSettingsByEntityAndName);

        for (Long oid: entities) {
            // settingSpecName-> Setting mapping
            Map<String, SettingAndPolicyIdRecord> settingsByName =
                userSettingsByEntityAndName.computeIfAbsent(
                    oid, k -> new HashMap<>());
            Map<String, Boolean> settingsWithSchedule =
                    settingsByEntityWithSchedule.computeIfAbsent(
                            oid, k -> new HashMap<>());

            for (SettingPolicy sp : settingPolicies) {
                final boolean policyHasSchedule = sp.getInfo().hasSchedule();
                if (inEffectNow(sp, scheduleResolver)) {
                    SettingPolicy.Type nextType = sp.getSettingPolicyType();
                    sp.getInfo().getSettingsList().forEach(
                        nextSetting -> resolve(nextSetting, sp.getId(), policyHasSchedule, nextType,
                            settingsByName, settingsWithSchedule, settingNameToSettingSpecs));
                }
            }
        }
    }

    /**
     * Policy is in effect now if it doesn't have a schedule (in which case it is always in
     * effect) or if it has a schedule and the schedule applies now.
     *
     * @param sp a setting policy with or without a schedule
     * @param scheduleResolver resolves whether a schedule applies
     * @return whether the policy is in effect
     */
    private static boolean inEffectNow(SettingPolicy sp, ScheduleResolver scheduleResolver) {
        return !sp.getInfo().hasSchedule()
                || scheduleResolver.appliesAtResolutionInstant(sp.getInfo().getSchedule());
    }

    private void resolve(Setting aSetting, long aPolicyId, boolean policyHasSchedule,
                    SettingPolicy.Type aType, Map<String, SettingAndPolicyIdRecord> settingsByName,
                    Map<String, Boolean> settingsWithSchedule,
                    Map<String, SettingSpec> settingNameToSettingSpecs) {
        final String specName = aSetting.getSettingSpecName();
        final SettingAndPolicyIdRecord existingRecord = settingsByName.get(specName);
        if (existingRecord != null) {
            final Setting conflictWinner = resolveSettingConflict(
                    existingRecord, aSetting, aType,
                    settingNameToSettingSpecs, settingsWithSchedule,
                    policyHasSchedule);
            if (conflictWinner != existingRecord.getSetting()) {
                settingsByName.put(specName, new SettingAndPolicyIdRecord(conflictWinner, aPolicyId, aType));
            }
        } else {
            settingsByName.put(specName, new SettingAndPolicyIdRecord(aSetting, aPolicyId, aType));
            settingsWithSchedule.put(specName, policyHasSchedule);
        }
    }

    /**
     * Determine which of an existing setting and a new setting, with the same spec name but
     * different values, should apply to an entity. If one is USER defined and one is DISCOVERED
     * then the USER defined policy wins. If one has a schedule and the other doesn't, the one
     * with a schedule wins. If both have a schedule or both don't, their spec's tie-breaker
     * is used to resolve the conflict.

     * @param existingRecord a previously found record for an entity
     * @param aSetting a new setting for an entity
     * @param aType the policy type (USER/DISCOVERED) of the policy that contains
     *     new setting
     * @param settingNameToSettingSpecs map of setting names to associated specs
     * @param settingsWithSchedule map of which existing settings are scheduled
     * @param policyHasSchedule whether the new setting is scheduled
     * @return whichever of the newSetting or the existingSetting that takes priority
     */
    private static Setting resolveSettingConflict(
                    final SettingAndPolicyIdRecord existingRecord,
                    final Setting aSetting, SettingPolicy.Type aType,
                    final Map<String, SettingSpec> settingNameToSettingSpecs,
                    final Map<String, Boolean> settingsWithSchedule,
                    final boolean policyHasSchedule) {
        // First check USER vs. DISCOVERED type and pick the USER policy regardless of other
        final Setting resultSetting;
        // USER policies win over DISCOVERED policies
        SettingPolicy.Type existingType = existingRecord.getType();
        if (existingType == SettingPolicy.Type.DISCOVERED
                        && aType == SettingPolicy.Type.USER) {
            resultSetting = aSetting;
        } else if (existingType == SettingPolicy.Type.USER
                        && aType == SettingPolicy.Type.DISCOVERED) {
            resultSetting = existingRecord.getSetting();
        } else {
            // Now we have two policies that are either both USER policies or
            // both DISCOVERED policies (latter is not expected to happen).
            // Policies with a schedule win over policies without a schedule.
            final String specName = aSetting.getSettingSpecName();
            final boolean existingSettingHasSchedule =
                            settingsWithSchedule.getOrDefault(specName, false);
            if (existingSettingHasSchedule && !policyHasSchedule) {
                resultSetting = existingRecord.getSetting();
            } else if (policyHasSchedule && !existingSettingHasSchedule) {
                resultSetting = aSetting;
                settingsWithSchedule.put(specName, true);
            } else  {
                logger.debug("Applying tiebreaker to settings: {} and {}", aSetting, existingRecord.getSetting());
                resultSetting = applyTiebreaker(aSetting, existingRecord.getSetting(),
                    settingNameToSettingSpecs);
                settingsWithSchedule.put(specName, policyHasSchedule);
            }
        }
        return resultSetting;
    }

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
    public static Setting applyTiebreaker(
                                @Nonnull Setting setting1,
                                @Nonnull Setting setting2,
                                @Nonnull Map<String, SettingSpec> settingNameToSettingSpecs) {

        Preconditions.checkArgument(!settingNameToSettingSpecs.isEmpty(),
            "Empty setting specs");

        String specName1 = setting1.getSettingSpecName();
        String specName2 = setting2.getSettingSpecName();
        Preconditions.checkArgument(
            specName1.equals(specName2), "Settings have different spec names");

        Preconditions.checkArgument(
            hasSameValueTypes(setting1, setting2), "Settings have different value types");

        SettingSpec spec1 = settingNameToSettingSpecs.get(specName1);
        Preconditions.checkArgument(spec1.hasEntitySettingSpec(),
                    "SettingSpec should be of type EntitySettingSpec");

        Preconditions.checkArgument(
                settingNameToSettingSpecs.get(specName2).hasEntitySettingSpec(),
                    "SettingSpec should be of type EntitySettingSpec");

        // Verified above that both settings are of same type. Hence they should
        // both have the same tie-breaker. So just extract it from one of the setting.
        SettingTiebreaker tieBreaker = spec1.getEntitySettingSpec().getTiebreaker();
        int ret = compareSettingValues(setting1, setting2, spec1);
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
    private static int compareSettingValues(Setting setting1,
                                     Setting setting2,
                                     SettingSpec settingSpec) {

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

    private static boolean hasSameValueTypes(Setting setting1, Setting setting2) {
        return setting1.getValueCase() == setting2.getValueCase();
    }

    /**
     * Create EntitySettings message.
     *
     * @param entity {@link TopologyEntity} whose settings should be created.
     * @param userSettings List of user Setting
     * @param defaultSettingPoliciesByEntityType Mapping of entityType to SettingPolicyId
     * @param settingOverrides The map of overrides, by setting name. See
           {@link EntitySettingsResolver#resolveSettings(GroupResolver, TopologyGraph<TopologyEntity>, SettingOverrides, TopologyInfo)}
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
                                .setSettingPolicyId(settingRecord.getSettingPolicyId())
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
    private Map<Long, Group> getGroupInfo(GroupServiceBlockingStub groupServiceClient,
                                          Collection<Long> groupIds) {

        final Map<Long, Group> groups = new HashMap<>();

        if (groupIds.isEmpty()) {
            return groups;
        }

        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .addAllId(groupIds)
            .setResolveClusterSearchFilters(true) // precalculate any cluster filters for us
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
     * Helper class to store the setting and the setting policy ID it is associated with.
     *
     */
    @VisibleForTesting
    class SettingAndPolicyIdRecord {

        private final Setting setting;
        private final long settingPolicyId;
        private final SettingPolicy.Type type;

        SettingAndPolicyIdRecord(final Setting setting, long settingPolicyId, SettingPolicy.Type type) {
            this.setting = setting;
            this.settingPolicyId = settingPolicyId;
            this.type = type;
        }

        public Setting getSetting() {
            return setting;
        }

        public long getSettingPolicyId() {
            return settingPolicyId;
        }

        public SettingPolicy.Type getType() {
            return type;
        }
    }
}
