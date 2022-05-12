package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.util.K8sProcessingUtil;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * A {@link CloneContext} that stores shared information for all clones.
 * The context is immutable, but objects inside it may be mutable.
 */
public class CloneContext {
    @Nonnull private final IdentityProvider idProvider;
    @Nonnull private final Map<Long, TopologyEntity.Builder> topology;
    private final String planType;
    private final long planId;
    @Nullable private final TopologyEntity.Builder planCluster;
    @Nullable private String planClusterVendorId;

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
     * A map of node commodities used to determine if an added pod should keep or drop its
     * commodities.
     */
    private final Map<CommodityType, Set<String>> nodeCommodities = new HashMap<>();
    /**
     * A cache that maps original entity ID to cloned entity ID for a given clone counter.
     */
    private final Map<Long, Map<Long, Long>> origToClonedIdMap = new HashMap<>();

    private CloneContext(@Nonnull final String planType,
                         final long planId,
                         @Nonnull final IdentityProvider idProvider,
                         @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                         @Nullable final TopologyEntity.Builder planCluster) {
        this.planType = planType;
        this.planId = planId;
        this.idProvider = idProvider;
        this.topology = topology;
        this.planCluster = planCluster;
        if (planCluster != null) {
            nodeCommodities.putAll(K8sProcessingUtil.collectNodeCommodities(planCluster));
            planClusterVendorId = TopologyEditorUtil.getContainerClusterVendorId(planCluster).orElse(null);
        }
        this.shouldApplyConstraints = FeatureFlags.APPLY_CONSTRAINTS_IN_CONTAINER_CLUSTER_PLAN.isEnabled()
                && StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN.equals(planType);
        this.isMigrateContainerWorkloadPlan = FeatureFlags.MIGRATE_CONTAINER_WORKLOAD_PLAN.isEnabled()
                && StringConstants.MIGRATE_CONTAINER_WORKLOADS_PLAN.equals(planType);
    }

    /**
     * Create a clone context to be shared by a particular clone.
     *
     * @param topologyInfo the topology info
     * @param idProvider the identity provider
     * @param topology the topology map
     * @return a clone context
     */
    public static CloneContext createContext(@Nonnull final TopologyInfo topologyInfo,
                                             @Nonnull final IdentityProvider idProvider,
                                             @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                             @Nullable final PlanScope scope) {
        // The plan type should always be set. Even if it is not set, it will default to empty string
        final String planType = topologyInfo.getPlanInfo().getPlanType();
        final long planId = topologyInfo.getTopologyContextId();
        final TopologyEntity.Builder planCluster = TopologyEditorUtil
                .getContainerCluster(topologyInfo, scope, topology)
                .orElse(null);
        return new CloneContext(planType, planId, idProvider, topology, planCluster);
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

    @Nonnull
    final Map<Long, TopologyEntity.Builder> getTopology() {
        return topology;
    }
}
