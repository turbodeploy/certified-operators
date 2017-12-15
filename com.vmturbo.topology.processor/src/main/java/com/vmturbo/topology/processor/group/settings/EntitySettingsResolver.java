package com.vmturbo.topology.processor.group.settings;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.SettingDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Responsible for resolving the Entities -> Settings mapping as well as
 * applying settings which transform the entities in the topology.
 * One example where the Settings application would lead to transformation is
 * derived calculations:
 *      Sometimes a probe specifies a formula for calculating capacity
 *      e.g. value * multiply_by_factor(Memory_Provisioned_Factor,
 *      CPU_Provisioned_Factor, Storage_Provisoned_Factor).
 *      The actual settings values for the multiply_by_factor has to be applied
 *      by the TP.
 *
 */
public class EntitySettingsResolver {

    private static final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private final GroupServiceBlockingStub groupServiceClient;

    private final SettingServiceBlockingStub settingServiceClient;

    /**
     * Create a new settings manager.
     *
     * @param settingPolicyServiceClient The service to use to retrieve setting policy definitions.
     * @param groupServiceClient The service to use to retrieve group definitions.
     * @param settingServiceClient The service to use to retrieve setting service definitions.
     */
    public EntitySettingsResolver(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceClient,
                                  @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                  @Nonnull final SettingServiceBlockingStub settingServiceClient) {
        this.settingPolicyServiceClient = Objects.requireNonNull(settingPolicyServiceClient);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.settingServiceClient = Objects.requireNonNull(settingServiceClient);
    }


    /**
     *  Resolve the groups associated with the SettingPolicies and associate the
     *  entities with their settings.
     *
     *  Do conflict resolution when an Entity has the same Setting from
     *  different SettingPolicies.
     *
     *  @param groupResolver Group resolver to resolve the groups associated with the settings.
     *  @param topologyGraph The topology graph on which to do the search.
     *  @param settingOverrides These overrides get applied after regular setting resolution
     *                          (including conflict resolution), so all entities that have these settings
     *                          will have the requested values. For example, if "move" is overriden
     *                          to "DISABLED" then all entities that "move" applies to (e.g. VMs)
     *                          will have the "move" settings as "DISABLED" no matter what the
     *                          setting policies say. There is currently no scope to the overrides,
     *                          so all overrides are global.
     *  @return List of EntitySettings
     *
     */
    public GraphWithSettings resolveSettings(
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph topologyGraph,
            @Nonnull final SettingOverrides settingOverrides) {

        final List<SettingPolicy> allSettingPolicies =
            getAllSettingPolicies(settingPolicyServiceClient);

        final List<SettingPolicy> userSettingPolicies =
            SettingDTOUtil.extractUserSettingPolicies(allSettingPolicies);

        // groupId -> SettingPolicies mapping
        final Map<Long, List<SettingPolicy>> groupSettingPoliciesMap =
            getGroupSettingPolicyMapping(userSettingPolicies);

        final Map<Long, Group> groups =
            getGroupInfo(groupServiceClient, groupSettingPoliciesMap.keySet());

        // EntityId(OID) -> Map<<Settings.name, Setting> mapping
        final Map<Long, Map<String, Setting>> userSettingsByEntityAndName = new HashMap<>();
        // SettingSpecName -> SettingSpec
        final Map<String, SettingSpec> settingNameToSettingSpecs = getAllSettingSpecs();

        // For each group, resolve it to get its entities. Then apply the settings
        // from the SettingPolicies associated with the group to the resolved entities
        groupSettingPoliciesMap.forEach((groupId, settingPolicies) -> {
            // This may be inefficient as we are walking the graph everytime to resolve a group.
            // Resolving a bunch of groups at once(ORing of the group search parameters)
            // would probalby be more efficient.
            try {
                final Group group = groups.get(groupId);
                if (group != null ) {
                    resolve(groupResolver.resolve(group, topologyGraph),
                        settingPolicies, userSettingsByEntityAndName, settingNameToSettingSpecs);
                } else {
                    logger.error("Group {} does not exist.", groupId);
                }
            } catch (GroupResolutionException gre) {
                // should we throw an exception ?
                logger.error("Failed to resolve group with id: {}", groupId, gre);
            }
        });

        final List<SettingPolicy> defaultSettingPolicies =
                SettingDTOUtil.extractDefaultSettingPolicies(allSettingPolicies);

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
     *  Resolve settings for a set of entities. If the settings have same values, resolve
     *  conflict.
     *
     * @param entities List of entity OIDs
     * @param settingPolicies List of settings policies to be applied to the entities
     * @param userSettingsByEntityAndName The return parameter which
     *              maps an entityId with its associated settings indexed by the
     *              settingsSpecName
     * @param settingNameToSettingSpecs Map of SettingSpecName to SettingSpecs
     */
    @VisibleForTesting
    void resolve(Set<Long> entities,
               List<SettingPolicy> settingPolicies,
               Map<Long, Map<String, Setting>> userSettingsByEntityAndName,
               Map<String, SettingSpec> settingNameToSettingSpecs) {

        checkNotNull(userSettingsByEntityAndName);

        for (Long oid: entities) {
            // settingSpecName-> Setting mapping
            Map<String, Setting> settingsMap =
                userSettingsByEntityAndName.computeIfAbsent(
                    oid, k -> new HashMap<>());
            for (SettingPolicy sp : settingPolicies) {
                sp.getInfo().getSettingsList().forEach((setting) -> {
                    final String specName = setting.getSettingSpecName();
                    final Setting existingSetting = settingsMap.get(specName);
                    final Setting resultSetting;
                    if (existingSetting != null) {
                        //  When 2 Settings have the same name and different values, there is a conflict
                        logger.debug("Settings conflict : {} and {}", setting, existingSetting);
                        resultSetting = resolveConflict(setting, existingSetting,
                                settingNameToSettingSpecs);
                    } else {
                        resultSetting = setting;
                    }
                    settingsMap.put(specName, resultSetting);
                });
            }
        }
    }

    /**
     *  Resolve conflict when 2 settings have the same spec but
     *  different values.
     *
     *  The tie-breaker to resolve conflict is defined in the SettingSpec.
     *
     *  @param setting1 Setting message
     *  @param setting2 Setting message
     *  @param settingNameToSettingSpecs Mapping from SettingSpecName to SettingSpec
     *  @return Resolved setting which won the tieBreaker
     */
    public static Setting resolveConflict(
                                @Nonnull Setting setting1,
                                @Nonnull Setting setting2,
                                @Nonnull Map<String, SettingSpec> settingNameToSettingSpecs) {

        Preconditions.checkArgument(!settingNameToSettingSpecs.isEmpty(),
            "Empty setting specs");

        Preconditions.checkArgument(
            setting1.getSettingSpecName().equals(
                setting2.getSettingSpecName()), "Settings have different spec names");

        Preconditions.checkArgument(
            hasSameValueTypes(setting1, setting2), "Settings have different value types");

        Preconditions.checkArgument(
                settingNameToSettingSpecs.get(setting1.getSettingSpecName())
                    .hasEntitySettingSpec(),
                    "SettingSpec should be of type EntitySettingSpec");

        Preconditions.checkArgument(
                settingNameToSettingSpecs.get(setting2.getSettingSpecName())
                    .hasEntitySettingSpec(),
                    "SettingSpec should be of type EntitySettingSpec");

        // Verified above that both settings are of same type. Hence they should
        // both have the same tie-breaker. So just extract it from one of the setting.
        SettingTiebreaker tieBreaker =
            settingNameToSettingSpecs.get(setting1.getSettingSpecName())
                .getEntitySettingSpec().getTiebreaker();

        int ret = compareSettingValues(setting1, setting2,
                    settingNameToSettingSpecs.get(setting1.getSettingSpecName()));

        switch (tieBreaker) {
            case BIGGER:
                return (ret >= 0) ? setting1 : setting2;
            case SMALLER:
                return (ret <= 0) ? setting1 : setting2;
            default:
            // shouldn't reach here.
            throw new IllegalArgumentException("Illegal tiebraker value : " + tieBreaker);
        }
    }

    /**
     *  Compare two setting values.
     *
     *  No validation is done in this method. Assumes all the
     *  input types and values are correct.
     *
     *  @param setting1 Setting message
     *  @param setting2 Setting message
     *  @param settingSpec SettingSpec definiton referred by the setting
     *                      messages. Both input settings should have the same
     *                      settingSpec name
     *
     *  @return Positive, negative or zero integer where setting1 value is
     *          greater than, smaller than or equal to setting2 value respectively.
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
     *  Create EntitySettings message.
     *
     *  @param entity {@link TopologyEntity} whose settings should be created.
     *  @param userSettings List of user Setting
     *  @param defaultSettingPoliciesByEntityType Mapping of entityType to SettingPolicyId
     *  @param settingOverrides The map of overrides, by setting name. See
           {@link EntitySettingsResolver#resolveSettings(GroupResolver,
                   TopologyGraph, SettingOverrides)}.
     *  @return EntitySettings message
     *
     */
    private EntitySettings createEntitySettingsMessage(TopologyEntity entity,
                @Nonnull final Collection<Setting> userSettings,
                @Nonnull final Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType,
                @Nonnull final SettingOverrides settingOverrides) {

        final EntitySettings.Builder entitySettingsBuilder =
            EntitySettings.newBuilder()
                    .setEntityOid(entity.getOid());
        userSettings.forEach(entitySettingsBuilder::addUserSettings);

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
     *                     the settings.
     * @param entitiesSettings List of EntitySettings messages
     */
    public void sendEntitySettings(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final Collection<EntitySettings> entitiesSettings) {
        // For now, don't upload settings for non-realtime topologies.
        if (topologyInfo.getTopologyType().equals(TopologyType.REALTIME)) {
            final UploadEntitySettingsRequest.Builder request =
                UploadEntitySettingsRequest.newBuilder()
                    .setTopologyId(topologyInfo.getTopologyId())
                    .setTopologyContextId(topologyInfo.getTopologyContextId())
                    .addAllEntitySettings(entitiesSettings);

            try {
                settingPolicyServiceClient.uploadEntitySettings(request.build());
            } catch (StatusRuntimeException sre) {
                logger.error("Failed to upload EntitySettings map to group component"
                    + " for topology {}. Error:", topologyInfo, sre.getMessage());
            }
        }
    }

    /**
     * Get all SettingPolicies from Group Component (GC).
     *
     * @param settingPolicyServiceClient Client for communicating with SettingPolicyService.
     * @return List of Setting policies.
     *
     */
    private List<SettingPolicy> getAllSettingPolicies(
            SettingPolicyServiceBlockingStub settingPolicyServiceClient) {

        final List<SettingPolicy> settingPolicies = new LinkedList<>();
        settingPolicyServiceClient.listSettingPolicies(
            ListSettingPoliciesRequest.getDefaultInstance())
                .forEachRemaining(settingPolicies::add);

        return settingPolicies;
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
     *  from the GroupId to the list of setting policies associated with the
     *  group.
     *
     *  @param settingPolicies List of SettingPolicy
     *  @return Mapping of the groupId to SettingPolicy
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

    /** Query the GroupInfo from GroupComponent for the provided GroupIds.
     *
     *   @param groupServiceClient Client for communicating with Group Service
     *   @param groupIds List of groupIds whose Group definitions has to be fetched
     *   @return Map of groupId and its Group object
     */
    private Map<Long, Group> getGroupInfo(GroupServiceBlockingStub groupServiceClient,
                                          Collection<Long> groupIds) {

        final Map<Long, Group> groups = new HashMap<>();

        if (groupIds.isEmpty()) {
            return groups;
        }

        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .addAllId(groupIds)
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
}
