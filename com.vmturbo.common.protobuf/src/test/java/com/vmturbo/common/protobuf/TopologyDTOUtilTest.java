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
import com.vmturbo.platform.common.dto.CommonDTO;
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

    /**
     * Test {@link TopologyDTOUtil#convertCapacity}.
     */
    @Test
    public void testConvertCapacity() {
        List<TopologyEntityDTO> entities = Arrays.asList(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE).setOid(111)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setApplication(TypeSpecificInfo.ApplicationInfo.newBuilder()
                                .setHostingNodeCpuFrequency(2700))
                        .build()).build(),
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.CONTAINER_POD_VALUE).setOid(222)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setContainerPod(TypeSpecificInfo.ContainerPodInfo.newBuilder()
                                        .setHostingNodeCpuFrequency(2700))
                                .build()).build());
        for (TopologyEntityDTO t : entities) {
            if (t.getEntityType() == CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT_VALUE) {
                double capacity = TopologyDTOUtil.convertCapacity(t, CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE, 1000);
                Assert.assertEquals(capacity, 2700, 0.1);
            } else if (t.getEntityType() == CommonDTO.EntityDTO.EntityType.CONTAINER_POD_VALUE) {
                double capacity = TopologyDTOUtil.convertCapacity(t, CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE, 2700);
                Assert.assertEquals(capacity, 1000, 0.1);
            }
        }

        List<TopologyEntityDTO> otherEntities = Arrays.asList(TopologyEntityDTO.newBuilder()
                        // A non pod or application entity
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(333)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(TypeSpecificInfo.VirtualMachineInfo.newBuilder())
                                .build()).build(),
                TopologyEntityDTO.newBuilder()
                        // Application from app probe without cpu freq set
                        .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE).setOid(111)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setApplication(TypeSpecificInfo.ApplicationInfo.newBuilder())
                                .build()).build(),
                TopologyEntityDTO.newBuilder()
                        // Any other entity
                        .setEntityType(EntityType.UNKNOWN_VALUE).setOid(555).build());
        // All other cases should return the capacity unchanged
        for (TopologyEntityDTO t : otherEntities) {
            double capacity = TopologyDTOUtil.convertCapacity(t, CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE, 1000);
            Assert.assertEquals(capacity, 1000, 0.1);
        }
    }

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setOid(11L);
    }
}
