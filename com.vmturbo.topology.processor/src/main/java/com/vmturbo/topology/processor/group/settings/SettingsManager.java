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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.SettingDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

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
public class SettingsManager {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyFilterFactory topologyFilterFactory;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceClient;

    private final GroupServiceBlockingStub groupServiceClient;

    /**
     * Create a new settings manager.
     *
     * @param settingPolicyServiceClient The service to use to retrieve settings definitions.
     * @param groupServiceClient The service to use to retrieve group definitions.
     * @param topologyFilterFactory The factory to use when creating topology filters for group/cluster resolution.
     */
    public SettingsManager(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceClient,
                           @Nonnull final GroupServiceBlockingStub groupServiceClient,
                           @Nonnull final TopologyFilterFactory topologyFilterFactory) {

        this.settingPolicyServiceClient = Objects.requireNonNull(settingPolicyServiceClient);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.topologyFilterFactory = Objects.requireNonNull(topologyFilterFactory);
    }

    /**
     *  Resolve the groups associated with the SettingPolicies, associate the
     *  entities with their settings and send the entitySetting mapping to Group
     *  Component.
     *
     *  Do conflict resolution when an Entity has the same Setting from
     *  different SettingPolicies.
     *
     * @param groupResolver Group resolver to resolve the groups associated with the settings.
     * @param topologyGraph The topology graph on which to do the search.
     * @param topologyContextId The topology context ID.
     * @param topologyId The topology ID.
     */
    public void applyAndSendEntitySettings(@Nonnull final GroupResolver groupResolver,
                              @Nonnull final TopologyGraph topologyGraph,
                              long topologyContextId,
                              long topologyId) {

        List<EntitySettings> entitiesSettings =
            applySettings(groupResolver, topologyGraph);

        logger.info("Finished applying settings. Sending the entitySetting " +
                        "mapping of size {} to Group component",
                        entitiesSettings.size());
        sendEntitySettings(topologyId, topologyContextId, entitiesSettings);
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
     *  @return List of EntitySettings
     *
     **/
    @VisibleForTesting
    List<EntitySettings> applySettings(@Nonnull final GroupResolver groupResolver,
                                       @Nonnull final TopologyGraph topologyGraph) {

        List<SettingPolicy> allSettingPolicies =
            getAllSettingPolicies(settingPolicyServiceClient);

        List<SettingPolicy> userSettingPolicies =
            SettingDTOUtil.extractUserSettingPolicies(allSettingPolicies);

        // groupId -> SettingPolicies mapping
        Map<Long, List<SettingPolicy>> groupSettingPoliciesMap =
            getGroupSettingPolicyMapping(userSettingPolicies);

        final Map<Long, Group> groups =
            getGroupInfo(groupServiceClient, groupSettingPoliciesMap.keySet());

        // EntityId(OID) -> Map<<Settings.name, Setting> mapping
        Map<Long, Map<String, Setting>> userSettingsByEntityAndName = new HashMap<>();

        // For each group, resolve it to get its entities. Then apply the settings
        // from the SettingPolicies associated with the group to the resolved entities
        groupSettingPoliciesMap.forEach((groupId, settingPolicies) -> {
            // This may be inefficient as we are walking the graph everytime to resolve a group.
            // Resolving a bunch of groups at once(ORing of the group search parameters)
            // would probalby be more efficient.
            try {
                apply(groupResolver.resolve(groups.get(groupId), topologyGraph),
                    settingPolicies, userSettingsByEntityAndName);
            } catch (GroupResolutionException gre) {
                // should we throw an exception ?
                logger.error("Failed to resolve group with id: {}", groupId, gre);
            }
        });

        // entityType -> SettingPolicyId mapping
        Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType =
            SettingDTOUtil.arrangeByEntityType(
                SettingDTOUtil.extractDefaultSettingPolicies(allSettingPolicies));

        // We have applied all the user settings. Now traverse the graph and
        // for each entity, associate its user settings and default setting policy id.
        // Group Component will look at the user settings and for the missing
        // settings, it will use the default settings which is defined in the
        // default SP.
        return topologyGraph.vertices()
                .map(vertex ->
                    createEntitySettingsMessage(vertex,
                        userSettingsByEntityAndName.getOrDefault(vertex.getOid(),
                                        Collections.emptyMap()).values(),
                        defaultSettingPoliciesByEntityType))
                .collect(Collectors.toList());
    }

    /**
     *  Apply Settings to entities. If the settings have same values, resolve
     *  conflict.
     *
     * @param entities List of entity OIDs
     * @param settingPolicies List of settings policies to be applied to the entities
     * @param userSettingsByEntityAndName The return parameter which
     *              maps an entityId with its associated settings indexed by the
     *              settingsSpecName
     */
    @VisibleForTesting
    void apply(Set<Long> entities,
                      List<SettingPolicy> settingPolicies,
                      Map<Long, Map<String, Setting>> userSettingsByEntityAndName) {

        checkNotNull(userSettingsByEntityAndName);

        for (Long oid: entities) {
            // settingSpecName-> Setting mapping
            Map<String, Setting> settingsMap =
                userSettingsByEntityAndName.computeIfAbsent(
                    oid, k -> new HashMap<>());
            for (SettingPolicy sp : settingPolicies) {
                for (Setting setting : sp.getInfo().getSettingsList()) {
                    if (settingsMap.containsKey(setting.getSettingSpecName())) {
                        //  When 2 Settings have the same name and different values, there is a conflict
                        //  TODO: karthikt -  OM-25471. Do Conflict resolution here. For now,
                        //  TODO:   choose the 1st setting that is encountered
                    } else {
                        settingsMap.put(setting.getSettingSpecName(), setting);
                    }
                }
            }
        }
    }

    /**
     *  Create EntitySettings message.
     *
     *  @param vertex Topology graph vertex
     *  @param userSettings List of user Setting
     *  @param defaultSettingPoliciesByEntityType Mapping of entityType to SettingPolicyId
     *  @return EntitySettings message
     *
     */
    private EntitySettings createEntitySettingsMessage(Vertex vertex,
                @Nonnull Collection<Setting> userSettings,
                @Nonnull Map<Integer, SettingPolicy> defaultSettingPoliciesByEntityType) {

        EntitySettings.Builder entitySettingsBuilder =
            EntitySettings.newBuilder()
                    .setEntityOid(vertex.getOid())
                    // should be fine to set to an empty list instead of addding
                    // a special check for empty input list
                    .addAllUserSettings(userSettings);

        if (defaultSettingPoliciesByEntityType.containsKey(vertex.getEntityType())) {
            entitySettingsBuilder.setDefaultSettingPolicyId(
                defaultSettingPoliciesByEntityType.get(vertex.getEntityType()).getId());
        }

        return entitySettingsBuilder.build();
    }

    /**
     * Send entitySettings mapping to the Group component.
     *
     * @param topologyId The ID of the topology which was used to resolve the settings
     * @param topologyContextId   The context ID of the topology
     * @param entitiesSettings List of EntitySettings messages
     */
    public void sendEntitySettings(long topologyId,
                                   long topologyContextId,
                                   List<EntitySettings> entitiesSettings) {

        UploadEntitySettingsRequest.Builder request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContextId)
                .addAllEntitySettings(entitiesSettings);

        try {
            settingPolicyServiceClient.uploadEntitySettings(request.build());
        } catch (StatusRuntimeException sre) {
            logger.error("Failed to upload EntitySettings map to group component"
                + " for topologyId: {} and topologyContextId: {}",
                topologyId, topologyContextId, sre);
        }
    }

    /**
     * Get all SettingPolicies from Group Component(GC).
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
