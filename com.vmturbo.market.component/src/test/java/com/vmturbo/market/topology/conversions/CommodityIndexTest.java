package com.vmturbo.market.topology.conversions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CommodityIndexTest {

    private static long ENTITY = 1L;

    private static long PROVIDER = 2L;

    private static double SCALING_FACTOR = 1.5;

    private static int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private static int TYPE = 2;
    private static CommodityType COMM_TYPE = CommodityType.newBuilder()
        .setType(TYPE)
        .setKey("foo")
        .build();

    private static CommodityType COMM_TYPE_2 = CommodityType.newBuilder()
        .setType(TYPE)
        .setKey("bar")
        .build();

    @Test
    public void testPutAndRetrieveCommBought() {
        final CommodityBoughtDTO commBought1 = CommodityBoughtDTO.newBuilder()
            .setCommodityType(COMM_TYPE)
            .setScalingFactor(SCALING_FACTOR)
            .build();
        final CommodityBoughtDTO commBought2 = CommodityBoughtDTO.newBuilder()
            .setCommodityType(COMM_TYPE_2)
            .setScalingFactor(SCALING_FACTOR)
            .build();
        final CommodityIndex index = CommodityIndex.newFactory().newIndex();
        index.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(ENTITY)
            .setEntityType(ENTITY_TYPE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PROVIDER)
                .addCommodityBought(commBought1)
                .addCommodityBought(commBought2))
            .build());

        // Test getting specific commodity.
        assertThat(index.getCommBought(ENTITY, PROVIDER, COMM_TYPE).get(),
            is(commBought1));
        assertThat(index.getCommBought(ENTITY, PROVIDER, COMM_TYPE_2).get(),
            is(commBought2));

        // Test each possible missing argument.
        assertFalse(index.getCommBought(ENTITY + 1, PROVIDER, COMM_TYPE).isPresent());
        assertFalse(index.getCommBought(ENTITY, PROVIDER + 1, COMM_TYPE).isPresent());
        assertFalse(index.getCommBought(ENTITY, PROVIDER, COMM_TYPE.toBuilder()
            .setType(TYPE + 1)
            .build()).isPresent());
    }

    @Test
    public void testPutAndRetrieveCommSold() {
        final CommoditySoldDTO commSold1 = CommoditySoldDTO.newBuilder()
            .setCommodityType(COMM_TYPE)
            .setScalingFactor(SCALING_FACTOR)
            .build();
        final CommoditySoldDTO commSold2 = CommoditySoldDTO.newBuilder()
            .setCommodityType(COMM_TYPE_2)
            .setScalingFactor(SCALING_FACTOR)
            .build();

        final CommodityIndex index = CommodityIndex.newFactory().newIndex();
        index.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(ENTITY)
            .setEntityType(ENTITY_TYPE)
            .addCommoditySoldList(commSold1)
            .addCommoditySoldList(commSold2)
            .build());

        assertThat(index.getCommSold(ENTITY, COMM_TYPE).get(), is(commSold1));
        assertThat(index.getCommSold(ENTITY, COMM_TYPE_2).get(), is(commSold2));

        assertFalse(index.getCommSold(ENTITY + 1, COMM_TYPE).isPresent());

        assertFalse(index.getCommSold(ENTITY, COMM_TYPE.toBuilder()
            .setType(TYPE + 1)
            .build()).isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPutIllegalCommBought() {
        final CommodityIndex index = CommodityIndex.newFactory().newIndex();
        index.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(ENTITY)
            .setEntityType(ENTITY_TYPE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PROVIDER)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMM_TYPE)
                    .setScalingFactor(SCALING_FACTOR)
                    .build())
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMM_TYPE)
                    .setScalingFactor(SCALING_FACTOR)
                    .build()))
            .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPutIllegalCommSold() {
        final CommodityIndex index = CommodityIndex.newFactory().newIndex();
        index.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(ENTITY)
            .setEntityType(ENTITY_TYPE)
            // Add the same commodity sold multiple times.
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(COMM_TYPE)
                .setScalingFactor(SCALING_FACTOR)
                .build())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(COMM_TYPE)
                .setScalingFactor(SCALING_FACTOR)
                .build())
            .build());
    }

}