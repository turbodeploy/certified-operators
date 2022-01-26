package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * The {@link ContainerPodCloneEditor} implements the clone function for the container pod and all
 * containers running in the pod.
 */
public class ContainerPodCloneEditor extends DefaultEntityCloneEditor {

    private static final Logger logger = LogManager.getLogger();

    ContainerPodCloneEditor(@Nonnull final TopologyInfo topologyInfo,
                            @Nonnull final IdentityProvider identityProvider) {
        super(topologyInfo, identityProvider);
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
}
