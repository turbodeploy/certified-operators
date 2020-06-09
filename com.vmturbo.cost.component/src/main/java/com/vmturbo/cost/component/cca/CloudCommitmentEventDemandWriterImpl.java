package com.vmturbo.cost.component.cca;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocationDatapoint.Builder;
import com.vmturbo.cloud.commitment.analysis.writer.CloudCommitmentDemandWriter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;

/**
 * Class used for constructing and filtering the allocation demand for a workload to be recorded to
 * the db.
 */
public class CloudCommitmentEventDemandWriterImpl implements CloudCommitmentDemandWriter {

    private final Logger logger = LogManager.getLogger();

    private final ComputeTierAllocationStore sqlComputeTierAllocationStore;

    private final boolean recordCloudAllocationData;

    /**
     * Constructs the CloudCommitmentEventDemandWriter.
     *
     * @param sqlComputeTierAllocationStore The sqlComputeTierAllocationStore which actually does
     *                                      the db transactions.
     * @param recordAllocationData A configurable boolean specifying whether we want to record
     *                             allocation demand,
     */
    public CloudCommitmentEventDemandWriterImpl(@Nonnull final SQLComputeTierAllocationStore sqlComputeTierAllocationStore,
                                                @Nonnull final Boolean recordAllocationData) {
        this.sqlComputeTierAllocationStore = sqlComputeTierAllocationStore;
        this.recordCloudAllocationData = recordAllocationData;
    }

    @Override
    public void writeAllocationDemand(final CloudTopology cloudTopology, final TopologyInfo topologyInfo) {
        if (recordCloudAllocationData) {
            final List<TopologyEntityDTO> listOfWorkloadsToBeUpdated = filterWorkloads(cloudTopology);
            final List<ComputeTierAllocationDatapoint> allocationDataPointsPersisted = new ArrayList<>();
            for (TopologyEntityDTO entity : listOfWorkloadsToBeUpdated) {
                Optional<ComputeTierAllocationDatapoint> computeTierAllocationDatapoint =
                        buildComputeTierAllocationDatapoint(entity, cloudTopology);
                if (computeTierAllocationDatapoint.isPresent()) {
                    allocationDataPointsPersisted.add(computeTierAllocationDatapoint.get());
                } else {
                    logger.error("No allocation datapoint could be constructed for entity with"
                            + " name {} and oid {}", entity.getDisplayName(), entity.getOid());
                }
            }
            sqlComputeTierAllocationStore.persistAllocations(topologyInfo, allocationDataPointsPersisted);
        }
    }

    /**
     * Filters workloads based on entity state and removes spot VM's.
     *
     * @param cloudTopology The cloud topology.
     *
     * @return A list of workloads to record allocation demand for.
     */
    private List<TopologyEntityDTO> filterWorkloads(CloudTopology cloudTopology) {
        final List<TopologyEntityDTO> workloads = cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        final List<TopologyEntityDTO> filteredVms = new ArrayList<>();
        for (TopologyEntityDTO entity: workloads) {
            if (EntityState.POWERED_ON == entity.getEntityState()) {
                if (entity.hasTypeSpecificInfo() && entity.getTypeSpecificInfo().hasVirtualMachine()) {
                    VirtualMachineInfo vmConfig = entity.getTypeSpecificInfo().getVirtualMachine();
                    if (vmConfig.getBillingType() != VMBillingType.BIDDING) {
                        filteredVms.add(entity);
                    }
                }
            }
        }
        return filteredVms;
    }

    /**
     * This builds a ComputeTierAllocationDatapoint for a particular entity dto.
     *
     * @param entityDTO The Entity to build the data point for.
     * @param cloudTopology The cloud topology.

     * @return The ComputeTierAllocationDatapoint.
     */
    public Optional<ComputeTierAllocationDatapoint> buildComputeTierAllocationDatapoint(TopologyEntityDTO entityDTO,
                                                                              CloudTopology cloudTopology) {
        final Builder datapointBuilder = ImmutableComputeTierAllocationDatapoint.builder();
        long entityOid = entityDTO.getOid();
        datapointBuilder.entityOid(entityOid);

        // Set the service provider oid
        Optional<TopologyEntityDTO> serviceProvider = cloudTopology.getServiceProvider(entityOid);
        serviceProvider.map(sp -> datapointBuilder.serviceProviderOid(sp.getOid()));

        // Set the region oid
        Optional<TopologyEntityDTO> region = cloudTopology.getConnectedRegion(entityOid);
        region.map(reg -> datapointBuilder.regionOid(reg.getOid()));

        // Set the availability zone oid
        Optional<TopologyEntityDTO> availabilityZone = cloudTopology.getConnectedAvailabilityZone(entityOid);
        availabilityZone.map(aZ -> datapointBuilder.availabilityZoneOid(aZ.getOid()));

        // Set the account oid
        Optional<TopologyEntityDTO> businessAccount = cloudTopology.getOwner(entityOid);
        businessAccount.map(ba -> datapointBuilder.accountOid(ba.getOid()));

        ImmutableComputeTierAllocation.Builder computeTierAllocationDemandBuilder = ImmutableComputeTierAllocation.builder();

        // Set the cloud tier on the compute tier allocation demand builder
        Optional<TopologyEntityDTO> cloudTier = cloudTopology.getComputeTier(entityDTO.getOid());
        cloudTier.map(ct -> computeTierAllocationDemandBuilder.cloudTierOid(ct.getOid()));

        if (entityDTO.hasTypeSpecificInfo() && entityDTO.getTypeSpecificInfo().hasVirtualMachine()) {
            VirtualMachineInfo vmInfo = entityDTO.getTypeSpecificInfo().getVirtualMachine();

            // Set the tenancy
            if (vmInfo.hasTenancy()) {
                computeTierAllocationDemandBuilder.tenancy(vmInfo.getTenancy());
            }

            // Set the OS Type
            if (vmInfo.hasGuestOsInfo() && vmInfo.getGuestOsInfo().hasGuestOsType()) {
                computeTierAllocationDemandBuilder.osType(vmInfo.getGuestOsInfo().getGuestOsType());
            }
        }
        datapointBuilder.cloudTierDemand(computeTierAllocationDemandBuilder.build());
        try {
            logger.debug("The builder for entity {} with oid {} is {}", entityDTO.getDisplayName(),
                    entityDTO.getOid(), datapointBuilder);
            return Optional.of(datapointBuilder.build());
        } catch (IllegalStateException e) {
            logger.debug("Exception encountered while building the compute tier allocation data point {}"
                    + "because one of the attributes wasn't set.", datapointBuilder, e);
            return Optional.empty();
        }
    }
}
