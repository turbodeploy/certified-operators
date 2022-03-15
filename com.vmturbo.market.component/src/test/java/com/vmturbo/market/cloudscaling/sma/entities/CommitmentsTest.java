package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Arrays;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Unit tests for {@link Commitments}.
 */
public class CommitmentsTest {

    /**
     * Test for {@link Commitments#getCommoditiesCommitmentAmountByTier}.
     */
    @Test
    public void testGetCommoditiesCommitmentAmountByTier() {
        Assert.assertEquals(ImmutableSet.of(CommittedCommodityBought.newBuilder().setCommodityType(
                CommodityDTO.CommodityType.MEM_PROVISIONED).setCapacity(321).build(),
                CommittedCommodityBought.newBuilder().setCommodityType(
                        CommodityDTO.CommodityType.NUM_VCORE).setCapacity(123).build()),
                ImmutableSet.copyOf(Commitments.getCommoditiesCommitmentAmountByTier(
                        TopologyEntityInfoExtractor::getComputeTierPricingCommoditiesStatic,
                        TopologyEntityDTO.newBuilder()
                                .setOid(1)
                                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                        .setComputeTier(
                                                ComputeTierInfo.newBuilder().setNumOfCores(123))
                                        .build())
                                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                        .setCommodityType(CommodityType.newBuilder()
                                                .setType(
                                                        CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE))
                                        .setCapacity(321))
                                .build()).get().getCommoditiesBought().getCommodityList()));
    }

    /**
     * Test for {@link Commitments#getCouponsCommitmentAmountByTier}.
     */
    @Test
    public void testGetCouponsCommitmentAmountByTier() {
        Assert.assertEquals(CloudCommitmentAmount.newBuilder().setCoupons(123).build(),
                Commitments.getCouponsCommitmentAmountByTier(TopologyEntityDTO.newBuilder()
                        .setOid(1)
                        .setEntityType(1)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setComputeTier(ComputeTierInfo.newBuilder().setNumCoupons(123))
                                .build())
                        .build()));
    }

    /**
     * Test for {@link Commitments#resolveCoveredAccountsByCommitment}.
     */
    @Test
    public void testResolveCoveredAccountsByCommitment() {
        Assert.assertEquals(ImmutableSet.of(2L, 3L, 4L),
                Commitments.resolveCoveredAccountsByCommitment(TopologyEntityDTO.newBuilder()
                        .setOid(1)
                        .setEntityType(1)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(2)
                                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                .setConnectionType(ConnectionType.NORMAL_CONNECTION))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(3)
                                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                .setConnectionType(ConnectionType.NORMAL_CONNECTION))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(4)
                                .setConnectedEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                .setConnectionType(ConnectionType.NORMAL_CONNECTION))
                        .build()));
    }

    /**
     * Test for {@link Commitments#getRIOidToEntityCoverage}.
     */
    @Test
    public void testGetRIOidToEntityCoverage() {
        Assert.assertEquals(
                ImmutableMap.of(1L, CloudCommitmentAmount.newBuilder().setCoupons(10D).build(), 2L,
                        CloudCommitmentAmount.newBuilder().setCoupons(20D).build(), 3L,
                        CloudCommitmentAmount.newBuilder().setCoupons(96D).build(), 4L,
                        CloudCommitmentAmount.newBuilder().setCoupons(40D).build()),
                Commitments.getRIOidToEntityCoverage(CommitmentAmountCalculator::sum,
                        ImmutableMap.of(1L, 10D, 2L, 20D, 3L, 31D),
                        Arrays.asList(CloudCommitmentMapping.newBuilder()
                                .setCloudCommitmentOid(3)
                                .setCommitmentAmount(
                                        CloudCommitmentAmount.newBuilder().setCoupons(32D).build())
                                .build(), CloudCommitmentMapping.newBuilder()
                                .setCloudCommitmentOid(3)
                                .setCommitmentAmount(
                                        CloudCommitmentAmount.newBuilder().setCoupons(33D).build())
                                .build(), CloudCommitmentMapping.newBuilder()
                                .setCloudCommitmentOid(4)
                                .setCommitmentAmount(
                                        CloudCommitmentAmount.newBuilder().setCoupons(40D).build())
                                .build())));

        Assert.assertEquals(ImmutableMap.of(3L, CloudCommitmentAmount.newBuilder()
                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityDTO.CommodityType.NUM_VCORE)
                                .setCapacity(40D)))
                .build()), Commitments.getRIOidToEntityCoverage(CommitmentAmountCalculator::sum,
                Collections.emptyMap(), Arrays.asList(CloudCommitmentMapping.newBuilder()
                        .setCloudCommitmentOid(3)
                        .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                        .addCommodity(CommittedCommodityBought.newBuilder()
                                                .setCommodityType(
                                                        CommodityDTO.CommodityType.NUM_VCORE)
                                                .setCapacity(20D))))
                        .build(), CloudCommitmentMapping.newBuilder()
                        .setCloudCommitmentOid(3)
                        .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                        .addCommodity(CommittedCommodityBought.newBuilder()
                                                .setCommodityType(
                                                        CommodityDTO.CommodityType.NUM_VCORE)
                                                .setCapacity(20D))))
                        .build())));
    }

    /**
     * Test for {@link Commitments#computeVmCoverage}.
     */
    @Test
    public void testComputeVmCoverage() {
        final SMAReservedInstance r1 = Mockito.mock(SMAReservedInstance.class);
        final SMAReservedInstance r2 = Mockito.mock(SMAReservedInstance.class);
        final SMAReservedInstance r3 = Mockito.mock(SMAReservedInstance.class);
        final Pair<SMAReservedInstance, CloudCommitmentAmount> computedCoverage =
                Commitments.computeVmCoverage(CommitmentAmountCalculator::sum, ImmutableMap.of(1L,
                        CloudCommitmentAmount.newBuilder()
                                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                        .addCommodity(CommittedCommodityBought.newBuilder()
                                                .setCommodityType(
                                                        CommodityDTO.CommodityType.NUM_VCORE)
                                                .setCapacity(20D)))
                                .build(), 2L, CloudCommitmentAmount.newBuilder()
                                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                        .addCommodity(CommittedCommodityBought.newBuilder()
                                                .setCommodityType(
                                                        CommodityDTO.CommodityType.NUM_VCORE)
                                                .setCapacity(30D)))
                                .build()), ImmutableMap.of(1L, r1, 2L, r2, 3L, r3)).get();
        Assert.assertEquals(CloudCommitmentAmount.newBuilder()
                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityDTO.CommodityType.NUM_VCORE)
                                .setCapacity(50D)))
                .build(), computedCoverage.getSecond());
        Assert.assertTrue(ImmutableSet.of(r1, r2).contains(computedCoverage.getFirst()));
    }
}