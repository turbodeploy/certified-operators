package com.vmturbo.common.protobuf;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyDTOUtil}.
 */
public class TopologyDTOUtilTest {

    private static final double VCPU_CAPACITY = 2000;
    private static final int NUM_CPUS = 2;

    @Test
    public void testIsUnplaced() {
        final TopologyEntityDTO unplacedEntity = newEntity()
                // Unplaced commodity - no provider ID.
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder())
                .build();
        Assert.assertFalse(TopologyDTOUtil.isPlaced(unplacedEntity));
    }

    @Test
    public void testIsUnplacedNegativeOid() {
        final TopologyEntityDTO unplacedEntity = newEntity()
                // Unplaced commodity - negative provider ID (-ve ID is illegal)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(-1))
                .build();
        Assert.assertFalse(TopologyDTOUtil.isPlaced(unplacedEntity));
    }

    @Test
    public void testIsPlaced() {
        final TopologyEntityDTO placedEntity = newEntity()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(7L))
                .build();
        Assert.assertTrue(TopologyDTOUtil.isPlaced(placedEntity));
    }

    @Test
    public void testIsPlan() {
        Assert.assertTrue(TopologyDTOUtil.isPlan(TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.getDefaultInstance())
                .build()));
    }

    @Test
    public void testIsNotPlan() {
        Assert.assertFalse(TopologyDTOUtil.isPlan(TopologyInfo.newBuilder()
                .build()));
    }

    /**
     * Determine whether or not the topology described by a topology
     * is generated for an optimize cloud plan.
     */
    @Test
    public void testIsOptimizeCloudPlan() {
        Assert.assertTrue(TopologyDTOUtil.isOptimizeCloudPlan(TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                .build()));
    }

    /**
     * Determine whether or not the topology described by a topology
     * is generated for an optimize cloud plan.
     */
    @Test
    public void testIsNotOptimizeCloudPlan() {
        Assert.assertFalse(TopologyDTOUtil.isOptimizeCloudPlan(TopologyInfo.newBuilder()
                .build()));
        Assert.assertFalse(TopologyDTOUtil.isOptimizeCloudPlan(TopologyInfo.newBuilder()
                .setPlanInfo(PlanTopologyInfo.getDefaultInstance())
                .build()));
    }

    @Test
    public void testIsPlanByType() {
        Assert.assertTrue(TopologyDTOUtil.isPlanType(
                PlanProjectType.CLUSTER_HEADROOM,
                TopologyInfo.newBuilder()
                    .setPlanInfo(PlanTopologyInfo.newBuilder()
                        .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                    .build()));
    }

    @Test
    public void testIsNotPlanByType() {
        Assert.assertFalse(TopologyDTOUtil.isPlanType(
                PlanProjectType.USER,
                TopologyInfo.newBuilder()
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanProjectType(PlanProjectType.CLUSTER_HEADROOM))
                        .build()));
    }

    /**
     * Test whether entity types can play the role of primary tier.
     */
    @Test
    public void testIsPrimaryTier() {
        // with no consumer provided, always-primary tiers true, all other entity types false
        Assert.assertTrue(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.COMPUTE_TIER_VALUE));
        Assert.assertFalse(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.STORAGE_TIER_VALUE));
        Assert.assertFalse(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.DISK_ARRAY_VALUE));

        // with consumer provided, always-primary tiers true even if consumer entity does not use that tier
        Assert.assertTrue(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.VIRTUAL_VOLUME_VALUE,
            EntityType.COMPUTE_TIER_VALUE));
        Assert.assertTrue(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.VIRTUAL_MACHINE_VALUE,
            EntityType.COMPUTE_TIER_VALUE));

        // with consumer provided, true only when tier is primary for that specific consumer type
        Assert.assertTrue(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.VIRTUAL_VOLUME_VALUE,
            EntityType.STORAGE_TIER_VALUE));
        Assert.assertFalse(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.VIRTUAL_VOLUME_VALUE,
            EntityType.DISK_ARRAY_VALUE));
        Assert.assertFalse(TopologyDTOUtil.isPrimaryTierEntityType(EntityType.VIRTUAL_MACHINE_VALUE,
            EntityType.STORAGE_TIER_VALUE));
    }

    /**
     * Test makeActionTypeSpecificInfo for VM without numCPUs.
     */
    @Test
    public void testMakeActionTypeSpecificInfoForVMWithoutNumCpus() {
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(VCPU_CAPACITY))
            .build();
        Optional<Builder> actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(topologyEntityDTO);
        Assert.assertTrue(actionEntityInfo.isPresent());
        Assert.assertTrue(actionEntityInfo.get().hasVirtualMachine());
        Assert.assertFalse(actionEntityInfo.get().getVirtualMachine().hasCpuCoreMhz());
    }

    /**
     * Test makeActionTypeSpecificInfo for VM with numCPUs.
     */
    @Test
    public void testMakeVMActionTypeSpecificInfoWithNumCpus() {
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                    .setNumCpus(NUM_CPUS)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(VCPU_CAPACITY))
            .build();
        Optional<Builder> actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(topologyEntityDTO);
        Assert.assertTrue(actionEntityInfo.isPresent());
        Assert.assertTrue(actionEntityInfo.get().hasVirtualMachine());
        Assert.assertTrue(actionEntityInfo.get().getVirtualMachine().hasCpuCoreMhz());
        double expectedCpuCoreMhz = VCPU_CAPACITY / NUM_CPUS;
        Assert.assertEquals(expectedCpuCoreMhz, actionEntityInfo.get().getVirtualMachine().getCpuCoreMhz(), 0);
    }

    /**
     * Test makeActionTypeSpecificInfo for VM in unknown state with numCPUs. In this case, VM
     * topologyEntityDTO won't have cpuCoreMhz field populated in virtualMachineInfo.
     */
    @Test
    public void testMakeVMActionTypeSpecificInfoWithNumCpusInUnknownState() {
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityState(EntityState.UNKNOWN)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                    .setNumCpus(NUM_CPUS)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(VCPU_CAPACITY))
            .build();
        Optional<Builder> actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(topologyEntityDTO);
        Assert.assertTrue(actionEntityInfo.isPresent());
        Assert.assertTrue(actionEntityInfo.get().hasVirtualMachine());
        Assert.assertFalse(actionEntityInfo.get().getVirtualMachine().hasCpuCoreMhz());
    }

    /**
     * Test {@link TopologyDTOUtil#getPrimaryProviderIndex}.
     */
    @Test
    public void testGetPrimaryProviderIndex() {
        int entityType = EntityType.CONTAINER_POD_VALUE;
        List<Integer> providerTypes = Arrays.asList(EntityType.WORKLOAD_CONTROLLER_VALUE,
            EntityType.VIRTUAL_MACHINE_VALUE);
        Optional<Integer> primaryProviderIndex = TopologyDTOUtil.getPrimaryProviderIndex(entityType, providerTypes);
        Assert.assertTrue(primaryProviderIndex.isPresent());
        Assert.assertEquals(1, primaryProviderIndex.get().intValue());
    }

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setOid(11L);
    }
}
