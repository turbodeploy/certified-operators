package com.vmturbo.common.protobuf;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * Unit tests for {@link TopologyDTOUtil}.
 */
public class TopologyDTOUtilTest {

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

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setOid(11L);
    }
}
