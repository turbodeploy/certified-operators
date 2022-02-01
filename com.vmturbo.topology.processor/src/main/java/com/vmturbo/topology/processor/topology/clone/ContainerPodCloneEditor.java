package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * The {@link ContainerPodCloneEditor} implements the clone function for the container pod and all
 * containers running in the pod.
 */
public class ContainerPodCloneEditor extends DefaultEntityCloneEditor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Editor-wise flag to indicate whether to apply constraints.  True only if the corresponding
     * feature flag is enabled and this is a container cluster plan.
     */
    private final boolean shouldApplyConstraints;

    ContainerPodCloneEditor(@Nonnull final TopologyInfo topologyInfo,
                            @Nonnull final IdentityProvider identityProvider) {
        super(topologyInfo, identityProvider);
        shouldApplyConstraints = FeatureFlags.APPLY_CONSTRAINTS_IN_CONTAINER_CLUSTER_PLAN.isEnabled()
                && topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo().hasPlanType()
                && StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN.equals(topologyInfo.getPlanInfo().getPlanType());
    }

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityDTO.Builder podDTO,
                                        final long cloneCounter,
                                        @Nonnull final Map<Long, Builder> topology) {
        final TopologyEntity.Builder clonedPod = super.clone(podDTO, cloneCounter, topology);
        cloneContainers(clonedPod, topology, podDTO.getOid(), cloneCounter);
        return clonedPod;
    }

    /**
     * Create clones of consumer entities from corresponding cloned provider entity and add them in
     * the topology. This is specifically used to clone consumer entities when cloning a provider
     * entity so that plan result will take consumer data into consideration. Cloned consumers are
     * not movable.
     *
     * <p>For example, when adding ContainerPods in plan, we clone the corresponding consumer
     * Containers to calculate CPU/memory overcommitments for ContainerPlatformCluster.
     *
     * @param clonedPod Given cloned provider entity builder.
     * @param topology The entities in the topology, arranged by ID.
     * @param origPodId Original provider ID of the cloned provider entity.
     * @param cloneCounter Counter of the entity to be cloned to be used in the display
     *         name.
     */
    void cloneContainers(@Nonnull final TopologyEntity.Builder clonedPod,
                         @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                         final long origPodId,
                         final long cloneCounter) {
        final Map<Long, Long> origToClonedPodIdMap = new HashMap<>();
        origToClonedPodIdMap.put(origPodId, clonedPod.getOid());
        final Origin entityOrigin = clonedPod.getEntityBuilder().getOrigin();
        // Clone corresponding consumers of the given added entity
        for (TopologyEntity container : topology.get(origPodId).getConsumers()) {
            TopologyEntityDTO.Builder containerDTO = container.getTopologyEntityDtoBuilder();
            TopologyEntityDTO.Builder clonedContainerDTO =
                    internalClone(containerDTO, cloneCounter,
                                  new HashMap<>(), origToClonedPodIdMap)
                            .setOrigin(entityOrigin);
            // Set controllable and suspendable to false to avoid generating actions on cloned consumers.
            // Consider this as allocation model, where we clone a provider along with corresponding
            // consumer resources but we won't run further analysis on the cloned consumers.
            clonedContainerDTO.getAnalysisSettingsBuilder()
                    .setControllable(false)
                    .setSuspendable(false);
            // Set providerId to the cloned consumers to make sure cloned provider won't be suspended.
            TopologyEntity.Builder clonedContainer = TopologyEntity
                    .newBuilder(clonedContainerDTO)
                    .setClonedFromEntity(containerDTO)
                    .addProvider(clonedPod);
            topology.put(clonedContainerDTO.getOid(), clonedContainer);
            clonedPod.addConsumer(clonedContainer);
        }
    }

    @Override
    protected boolean shouldSkipProvider(
            @Nonnull CommoditiesBoughtFromProvider boughtFromProvider) {
        // As we aren't copying the related workload controller nor the volume into the plan, we
        // will skip those providers.
        return Objects.requireNonNull(boughtFromProvider).hasProviderEntityType()
                && (boughtFromProvider.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE
                || boughtFromProvider.getProviderEntityType() == EntityType.WORKLOAD_CONTROLLER_VALUE);
    }

    @Override
    protected boolean shouldCopyBoughtCommodity(@Nonnull CommodityBoughtDTO commodityBought) {
        // The override behavior is enforced only when the feature flag is enabled and this is the
        // container cluster plan.  In that case, we will copy except for the cluster commodity.
        // That is because there is no need for such a restriction for pods in the plan where
        // there's only one container cluster.  In the future when we support multiple clusters in
        // plan, we will revisit but this still seems a good choice not to restrict the pods in any
        // particular cluster.
        return super.shouldCopyBoughtCommodity(commodityBought) || (shouldApplyConstraints
                && CommodityType.CLUSTER_VALUE != commodityBought.getCommodityType().getType());
    }

    @Override
    protected boolean shouldReplaceBoughtKey(@Nonnull final TopologyDTO.CommodityType commodityType,
            final int providerEntityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && CommodityType.VMPM_ACCESS_VALUE == commodityType.getType()
                && EntityType.CONTAINER_POD_VALUE == providerEntityType;
    }

    @Override
    protected boolean shouldReplaceSoldKey(@Nonnull final TopologyDTO.CommodityType commodityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && CommodityType.VMPM_ACCESS_VALUE == commodityType.getType();
    }
}
