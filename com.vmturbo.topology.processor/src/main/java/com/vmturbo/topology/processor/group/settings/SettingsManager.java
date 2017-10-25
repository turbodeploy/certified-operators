package com.vmturbo.topology.processor.group.settings;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
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

    private final ClusterServiceBlockingStub clusterServiceClient;

    /**
     * Create a new settings manager.
     *
     * @param settingPolicyServiceClient The service to use to retrieve settings definitions.
     * @param groupServiceClient The service to use to retrieve group definitions.
     * @param clusterServiceClient The service to use to retrieve cluster definitions.
     * @param topologyFilterFactory The factory to use when creating topology filters for group/cluster resolution.
     */
    public SettingsManager(@Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceClient,
                           @Nonnull final GroupServiceBlockingStub groupServiceClient,
                           @Nonnull final ClusterServiceBlockingStub clusterServiceClient,
                           @Nonnull final TopologyFilterFactory topologyFilterFactory) {

        this.settingPolicyServiceClient = Objects.requireNonNull(settingPolicyServiceClient);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.clusterServiceClient = Objects.requireNonNull(clusterServiceClient);
        this.topologyFilterFactory = Objects.requireNonNull(topologyFilterFactory);
    }

    /**
     *  Resolve the groups associated with the SettingPolicies and return the EntitySettings mapping.
     *  Also do conflict resolution when an Entity has the same Setting from
     *  different SettingPolicies
     *
     * @param groupResolver Group resolver to resolve the groups associated with the settings
     * @param topologyGraph The topology graph on which to do the search
     * @return Mapping of entities and the list of settings associated with the entities
     */
    public Map<Long, List<Setting>> applySettings(@Nonnull final GroupResolver groupResolver,
                    @Nonnull final TopologyGraph topologyGraph) {

        // TODO: karthikt - handle defualt settings : OM-25470
        List<SettingPolicy> settingPoliciesList = getAllUserSettingPolicies(settingPolicyServiceClient);
        // groupId -> SettingPolicies mapping
        Map<Long, List<SettingPolicy>> groupSettingPoliciesMap =
            getGroupSettingPolicyMapping(settingPoliciesList);
        Map<Long, List<SettingPolicy>> clusterSettingPoliciesMap =
            getClusterSettingPolicyMapping(settingPoliciesList);

        final Map<Long, Group> groups =
            getGroupInfo(groupServiceClient, groupSettingPoliciesMap.keySet());
        final Map<Long, Cluster> clusters =
            getClusterInfo(clusterServiceClient, clusterSettingPoliciesMap.keySet());

        // EntityId(OID) -> Map<<Settings.name, Setting> mapping
        Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap = new HashMap<>();

        // For each group, resolve it to get its entities and apply the settings
        // from the SettingPolicies associated with the group to the resolved entities
        groupSettingPoliciesMap.forEach((groupId, settingPolicies) -> {
            // karthikt - This may be inefficient as we are walking the graph everytime to resolve a group.
            // karthikt - Maybe resolving a bunch of groups at once(ORing of the group search
            // karthikt - parameters) would help.
            try {
                apply(groupResolver.resolve(groups.get(groupId), topologyGraph),
                    settingPolicies, entitySettingsBySettingNameMap);
            } catch (GroupResolutionException gre) {
                // should we throw an exception ?
                logger.error("Failed to resolve group with id: {}", groupId, gre);
            }
        });

        // apply setting to cluster entities
        clusterSettingPoliciesMap.forEach((clusterId, settingPolicies) -> {
            apply(getClusterEntities(clusters.get(clusterId)),
                settingPolicies, entitySettingsBySettingNameMap);
        });

        Map<Long, List<Setting>> entitySettingsMap = new HashMap<>();
        entitySettingsBySettingNameMap.forEach((entityId, settingsMap) -> {
            entitySettingsMap.computeIfAbsent(
               entityId, k -> new LinkedList<Setting>()).addAll(settingsMap.values());
        });

        return entitySettingsMap;
    }

    /**
     *  Apply Settings to entities. If the settings have same values, resolve
     *  conflict.
     *
     * @param entities List of entity OIDs
     * @param settingPolicies List of settings policies to be applied to the entities
     * @param entitySettingsBySettingNameMap The return parameter which
     *              maps an entityId with its associated settings indexed by the
     *              settingsSpecName
     */
    public void apply(Set<Long> entities,
                      List<SettingPolicy> settingPolicies,
                      Map<Long, Map<String, Setting>> entitySettingsBySettingNameMap) {

        checkNotNull(entitySettingsBySettingNameMap);

        for (Long oid: entities) {
            // settingSpecName-> Setting mapping
            Map<String, Setting> settingsMap =
                entitySettingsBySettingNameMap.computeIfAbsent(
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
     * Send entitySettings mapping to the Group component.
     *
     * @param topologyId The ID of the topology which was used to resolve the settings
     * @param topologyContexId   The context ID of the topology
     * @param entitySettingsMap The map of EntityIds and their associated settings
     */
    public void sendEntitySettings(long topologyId,
                                   long topologyContexId,
                                   Map<Long, List<Setting>> entitySettingsMap) {

        UploadEntitySettingsRequest.Builder request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId);

        entitySettingsMap.forEach((key, value) -> {
            request.addEntitySettings(
                EntitySettings.newBuilder()
                    .setEntityOid(key)
                    .addAllSettings(value)
                    .build());
        });

        try {
            settingPolicyServiceClient.uploadEntitySettings(request.build());
        } catch (StatusRuntimeException sre) {
            logger.error("Failed to upload EntitySettings map to group component"
                + " for topologyId: {} and topologyContexId: {}",
                topologyId, topologyContexId, sre);
        }
    }

    /**
     * Return the list of entityOids which are part of the input cluster.
     *
     * @param cluster Cluster to extract the entities from
     * @return OIDs of the entities which are part of the cluster
     */
    public Set<Long> getClusterEntities(Cluster cluster) {
            return new HashSet<>(cluster
                            .getInfo()
                            .getMembers()
                            .getStaticMemberOidsList());
    }

    /**
     * Get all non-default SettingPolicies from Group Component(GC).
     *
     * @param settingPolicyServiceClient Client for communicating with SettingPolicyService.
     * @return List of User Setting policies.
     *
     */
    private List<SettingPolicy> getAllUserSettingPolicies(
        SettingPolicyServiceBlockingStub settingPolicyServiceClient) {

        List<SettingPolicy> settingPolicies = new LinkedList<>();

        settingPolicyServiceClient.listSettingPolicies(
            ListSettingPoliciesRequest.getDefaultInstance())
                .forEachRemaining(settingPolicy -> {
                    if (settingPolicy.hasSettingPolicyType() &&
                            (settingPolicy.getSettingPolicyType() == SettingPolicy.Type.USER) &&
                            settingPolicy.hasInfo() &&
                            settingPolicy.getInfo().hasEnabled() &&
                            settingPolicy.getInfo().getEnabled() &&
                            settingPolicy.getInfo().hasScope()) {

                                settingPolicies.add(settingPolicy);
                    } else {
                        logger.warn("SettingPolicy has missing fields : {}", settingPolicy);
                    }
                });

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

    /**
    * Extract the clusters which are part of the SettingPolicies and return a
    * mapping from the ClusterId to the list of setting policies associated
    * with the Cluster.
    *
    * @param settingPolicies List of SettingPolicy
    * @return Mapping of the clusterId to SettingPolicy
    */
    private Map<Long, List<SettingPolicy>> getClusterSettingPolicyMapping(
        List<SettingPolicy> settingPolicies) {

        Map<Long, List<SettingPolicy>> clusterSettingPoliciesMap = new HashMap<>();
        settingPolicies.forEach((settingPolicy) -> {
            settingPolicy.getInfo().getScope().getClustersList()
                .forEach((clusterId) -> {
                    clusterSettingPoliciesMap.computeIfAbsent(
                        clusterId, k -> new LinkedList<>()).add(settingPolicy);
                });
        });

        return clusterSettingPoliciesMap;
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

    /** Query the Cluster info from GroupComponent for the provided clusterIds.
     *
     *   @param clusterServiceClient Client for communicating with Cluster Service
     *   @param clusterIds List of clusterIds whose Cluster definitions has to be fetched
     *   @return Mapping of clusterId and its associated cluster object
     */
    private Map<Long, Cluster> getClusterInfo(ClusterServiceBlockingStub clusterServiceClient,
                                              Collection<Long> clusterIds) {

        final Map<Long, Cluster> clusters = new HashMap<>();

        if (clusterIds.isEmpty()) {
            return clusters;
        }

        clusterServiceClient.getClusters(GetClustersRequest.newBuilder()
                .addAllId(clusterIds)
                .build())
                .forEachRemaining(cluster ->  {
                    if (cluster.hasId()) {
                        clusters.put(cluster.getId(), cluster);
                    } else {
                        logger.warn("Cluster has no id. Skipping. {}", cluster);
                    }
                });

        return clusters;
    }
}
