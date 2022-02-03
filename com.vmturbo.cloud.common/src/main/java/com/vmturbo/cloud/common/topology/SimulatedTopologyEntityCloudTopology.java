package com.vmturbo.cloud.common.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.group.api.GroupMemberRetriever;

/**
 * A {@link CloudTopology} for {@link TopologyEntityDTO}, to be used when running the cost
 * library in the cost component.
 */
public class SimulatedTopologyEntityCloudTopology extends TopologyEntityCloudTopology {

    private Map<Long, Long> entityIdToComputeTierIdSimulation = new HashMap<>();

    /**
     * Creates an instance of TopologyEntityCloudTopology with the provided topologyEntities and
     * billingFamilies.
     *
     * @param topologyEntities stream of TopologyEntityDTOs from which the CloudTopology is
     *                         constructed.
     * @param groupMemberRetriever service object to retrieve billing families information.
     */
    public SimulatedTopologyEntityCloudTopology(
            @Nonnull final Stream<TopologyEntityDTO> topologyEntities,
            @Nonnull final GroupMemberRetriever groupMemberRetriever) {
        super(topologyEntities, groupMemberRetriever);
    }



    /**
     * The simulation is to simulate a VM moving to a compute tier. So that when we call the
     * get compute tier for a vm we would use the mapped value rather than the actual compute tier.
     *
     * @param entityIdToComputeTierIdSimulation the simulation map which will over write the existing simulation.
     */
    public void setEntityIdToComputeTierIdSimulation(
            @Nonnull Map<Long, Long> entityIdToComputeTierIdSimulation) {
        this.entityIdToComputeTierIdSimulation = entityIdToComputeTierIdSimulation;
    }

    /**
     * The compute tier is not the actual current compute tier of the entity.
     * We want to return the compute-tier which we have simulated the entity has moved to.
     * If the simulation map has an entry corresponding to entityId then we have simulated a
     * move. So return the compute tier corresponding to the simulated computeTier id.
     *
     * @param entityId the entiry for which we need the compute tier for.
     * @return the compute tier TopologyEntityDTO of the entity(vm)
     */
    @Override
    @Nonnull
    public Optional<TopologyEntityDTO> getComputeTier(final long entityId) {
        Long computeTierId = entityIdToComputeTierIdSimulation.get(entityId);
        if (computeTierId != null) {
            return super.getEntity(computeTierId);
        } else {
            return super.getComputeTier(entityId);
        }
    }
}
