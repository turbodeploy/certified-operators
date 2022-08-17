package com.vmturbo.topology.processor.topology.clone;

import static com.vmturbo.topology.processor.group.policy.PolicyManager.createAtMostNPlacementPolicy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.utilities.CPUScalingFactorUpdater;
import com.vmturbo.stitching.utilities.CPUScalingFactorUpdater.CloudNativeCPUScalingFactorUpdater;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.AtMostNPolicy;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.ConsistentScalingCache;
import com.vmturbo.topology.processor.util.K8sProcessingUtil;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * A {@link CloneContext} that stores shared information for all clones.
 * The context is immutable, but objects inside it may be mutable.
 */
public class CloneContext {
    private static final String PLAN_CLUSTER_NODE_GROUP_DESCRIPTION_SUFFIX = " provider node group";
    @Nonnull
    private final IdentityProvider idProvider;
    @Nonnull
    private final Map<Long, TopologyEntity.Builder> topology;
    private final String planType;
    private final long planId;
    @Nullable
    private final TopologyEntity.Builder planCluster;
    @Nullable
    private String planClusterVendorId;
    @Nonnull
    private final List<PlacementPolicy> placementPolicies;
    /**
     * Editor-wise flag to indicate whether to apply constraints.  True only if the corresponding
     * feature flag is enabled and this is a container cluster plan.
     */
    private final boolean shouldApplyConstraints;
    /**
     * Editor-wise flag to indicate whether to enable migrating container workload feature.
     */
    private final boolean isMigrateContainerWorkloadPlan;
    /**
     * The set of provider node oids in the plan cluster.  Examples of nodes that can't be a
     * provider include:
     * - master nodes that are marked SchedulingDisabled
     * - cordoned nodes that are also SchedulingDisabled
     */
    private final Set<Long> planClusterProviderNodeOids = new HashSet<>();
    /**
     * A group of nodes in the plan cluster that can be a provider.  Examples of those that can't
     * be a provider include:
     * - master nodes that are marked SchedulingDisabled
     * - cordoned nodes that are also SchedulingDisabled
     */
    private final Grouping planClusterProviderNodeGroup;
    /**
     * A map of node commodities used to determine if an added pod should keep or drop its
     * commodities.
     */
    private final Map<CommodityType, Set<String>> nodeCommodities = new HashMap<>();
    /**
     * A cache that maps original entity ID to cloned entity ID for a given clone counter.
     */
    private final Map<Long, Map<Long, Long>> origToClonedIdMap = new HashMap<>();

    private static final CPUScalingFactorUpdater cpuScalingFactorUpdater =
            new CloudNativeCPUScalingFactorUpdater();

    /**
     * A map that caches the clusters that have already updated cpuScalingFactor for the current
     * plan instance.
     */
    private final Map<TopologyEntity, Boolean> clustersWithCPUScalingFactorUpdated = new HashMap<>();

    /**
     * A cache of consistent scaling values.
     */
    private final ConsistentScalingCache consistentScalingCache;

    private CloneContext(@Nonnull final String planType, final long planId,
                         @Nonnull final IdentityProvider idProvider,
                         @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                         @Nullable final TopologyEntity.Builder planCluster,
                         @Nonnull final List<PlacementPolicy> placementPolicies) {
        this.planType = planType;
        this.planId = planId;
        this.idProvider = idProvider;
        this.topology = topology;
        this.planCluster = planCluster;
        if (planCluster != null) {
            nodeCommodities.putAll(K8sProcessingUtil.collectNodeCommodities(planCluster));
            planClusterVendorId = TopologyEditorUtil.getContainerClusterVendorId(planCluster)
                    .orElse(null);
            planClusterProviderNodeOids.addAll(K8sProcessingUtil.getProviderNodeOids(planCluster));
            planClusterProviderNodeGroup = PolicyManager.generateStaticGroup(
                    planClusterProviderNodeOids, EntityType.VIRTUAL_MACHINE_VALUE,
                    planCluster.getDisplayName() + PLAN_CLUSTER_NODE_GROUP_DESCRIPTION_SUFFIX);
        } else {
            planClusterProviderNodeGroup = null;
        }
        this.shouldApplyConstraints = FeatureFlags.APPLY_CONSTRAINTS_IN_CONTAINER_CLUSTER_PLAN.isEnabled()
                && StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN.equals(planType);
        this.isMigrateContainerWorkloadPlan = FeatureFlags.MIGRATE_CONTAINER_WORKLOAD_PLAN.isEnabled()
                && StringConstants.MIGRATE_CONTAINER_WORKLOADS_PLAN.equals(planType);
        this.consistentScalingCache = new ConsistentScalingCache();
        this.placementPolicies = Objects.requireNonNull(placementPolicies);
    }

    /**
     * Create a clone context to be shared by a particular clone.
     *
     * @param topologyInfo the topology info
     * @param idProvider the identity provider
     * @param topology the topology builder map
     * @param scope the scope of the plan
     * @param placementPolicies the placement policies to be created in the policy stage
     * @return a clone context
     */
    public static CloneContext createContext(@Nonnull final TopologyInfo topologyInfo,
                                             @Nonnull final IdentityProvider idProvider,
                                             @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                             @Nullable final PlanScope scope,
                                             @Nonnull final List<PlacementPolicy> placementPolicies
    ) {
        // The plan type should always be set. Even if it is not set, it will default to empty string
        final String planType = topologyInfo.getPlanInfo().getPlanType();
        final long planId = topologyInfo.getTopologyContextId();
        final TopologyEntity.Builder planCluster = TopologyEditorUtil
                .getContainerCluster(topologyInfo, scope, topology)
                .orElse(null);
        return new CloneContext(planType, planId, idProvider, topology, planCluster, placementPolicies);
    }

    /**
     * Get the plan cluster.
     *
     * @return an optional of plan cluster
     */
    Optional<TopologyEntity.Builder> getPlanCluster() {
        return Optional.ofNullable(planCluster);
    }

    /**
     * Get the plan cluster vendor ID.
     *
     * @return the plan cluster vendor ID
     */
    Optional<String> getPlanClusterVendorId() {
        return Optional.ofNullable(planClusterVendorId);
    }

    /**
     * Get the plan type associated with this clone factory.
     *
     * @return the plan type
     */
    @Nonnull
    String getPlanType() {
        return planType;
    }

    /**
     * Get the plan ID associated with this clone factory.
     *
     * @return the plan ID
     */
    long getPlanId() {
        return planId;
    }

    /**
     * Check if the current plan is a migrating container workload plan.
     *
     * @return true if the current plan is a migrating container workload plan
     */
    boolean isMigrateContainerWorkloadPlan() {
        return isMigrateContainerWorkloadPlan;
    }

    /**
     * Check if constraints should be applied for optimize container plan.
     *
     * @return true if constraints should be applied for optimize container plan
     */
    boolean shouldApplyConstraints() {
        return shouldApplyConstraints;
    }

    @Nonnull
    Map<CommodityType, Set<String>> getNodeCommodities() {
        return nodeCommodities;
    }

    /**
     * Cache cloned entity of the given clone counter for this plan run.
     *
     * @param cloneCounter counter of the entity to be cloned
     * @param originalId the original entity ID
     * @param clonedId the cloned entity ID
     */
    void cacheClonedEntityId(final long cloneCounter,
                             final long originalId,
                             final long clonedId) {
        origToClonedIdMap.computeIfAbsent(cloneCounter, k -> new HashMap<>())
                .put(originalId, clonedId);
    }

    /**
     * Get the cloned entity ID of the given clone counter from the cache.
     *
     * @param cloneCounter counter of the entity to be cloned
     * @param originalId the original entity ID
     * @return the cloned entity ID
     */
    @Nullable
    Long getClonedEntityId(final long cloneCounter,
                           final long originalId) {
        return origToClonedIdMap.computeIfAbsent(cloneCounter, k -> new HashMap<>())
                .get(originalId);
    }

    /**
     * Check if the original entity has already been cloned.
     *
     * @param cloneCounter counter of the entity to be cloned
     * @param originalId the original entity ID
     * @return true if the original entity has already been cloned
     */
    boolean isEntityCloned(final long cloneCounter,
                           final long originalId) {
        return origToClonedIdMap.computeIfAbsent(cloneCounter, k -> new HashMap<>())
                .containsKey(originalId);
    }

    /**
     * Get identity provider.
     *
     * @return the identity provider
     */
    @Nonnull
    IdentityProvider getIdProvider() {
        return idProvider;
    }

    /**
     * Get the topology map.
     *
     * @return the topology map
     */
    @Nonnull
    final Map<Long, TopologyEntity.Builder> getTopology() {
        return topology;
    }

    /**
     * Get the consistent scaling cache.
     *
     * @return the consistent scaling cache
     */
    @Nonnull
    final ConsistentScalingCache getConsistentScalingCache() {
        return consistentScalingCache;
    }

    /**
     * Update the cpu scaling factor for entities in a container cluster.
     * The operations are performed only once for each unique cluster for each plan instance.
     *
     * @param cluster the container cluster to update cpuScalingFactor
     */
    void updateCPUScalingFactor(@Nonnull final TopologyEntity cluster) {
        clustersWithCPUScalingFactorUpdated.computeIfAbsent(cluster, c -> {
            cluster.getAggregatedEntities().stream()
                    .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                    .forEach(vm -> cpuScalingFactorUpdater.update(vm, 1d, new LongOpenHashSet()));
            return true;
        });
    }

    /**
     * Return the number of nodes in the plan cluster.
     *
     * @return the number of nodes in the plan cluster
     */
    public int getPlanClusterNodeCount() {
        return planClusterProviderNodeOids.size();
    }

    /**
     * Return a {@link Grouping} representing the group of nodes in the clusters.  This is to
     * facilitate creating a placement policy for the migrated daemon entities such as container
     * pods in a daemon set.
     *
     * @return the node group in the plan cluster
     */
    public Grouping getPlanClusterProviderNodeGroup() {
        return planClusterProviderNodeGroup;
    }

    /**
     * Add a policy group for creating a placement policy in the later stage of the plan pipeline.
     *
     * @param consumerGroup the consumer {@link Grouping}
     * @param providerGroup the provider {@link Grouping}
     * @param n the "N" in the {@link AtMostNPolicy}
     */
    public void addPlacementPolicy(@Nonnull final Grouping consumerGroup,
            @Nonnull final Grouping providerGroup, final int n) {
        final PlacementPolicy policy = createAtMostNPlacementPolicy(
                Objects.requireNonNull(consumerGroup),
                Objects.requireNonNull(providerGroup), n);
        placementPolicies.add(policy);
    }
}
