package com.vmturbo.action.orchestrator.topology;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Verifies that ActionGraphEntity converts TopologyEntityDTO to ActionPartialEntity.
 */
public class ActionGraphEntityTest {

    /**
     * An ActionGraphEntity without sold commodities should not cause a failure.
     * Secondly, hot replace commodity types should be empty.
     */
    @Test
    public void testReplaceCommodityTypesFromNoSoldCommodities() {
        ActionGraphEntity actionGraphEntity = new Builder(TopologyEntityDTO.newBuilder()
                .buildPartial())
            .build();
        Assert.assertEquals(
                0,
                actionGraphEntity.asPartialEntity().getCommTypesWithHotReplaceCount());
    }

    /**
     * An ActionGraphEntity with sold commodities but no hot resize info should not have any
     * hot replace commodity types.
     */
    @Test
    public void testReplaceCommodityTypesFromNoHotResizeInfo() {
        ActionGraphEntity actionGraphEntity = new Builder(TopologyEntityDTO.newBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
                            .build())
                    .buildPartial())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                            .build())
                    .buildPartial())
            .buildPartial())
            .build();
        Assert.assertEquals(
            0,
            actionGraphEntity.asPartialEntity().getCommTypesWithHotReplaceCount());
    }

    /**
     * An ActionGraphEntity with sold commodities but none of the hot resize info have hot replace
     * set to true should not have any hot replace commodity types.
     */
    @Test
    public void testReplaceCommodityTypesFromHasHotResizeInfo() {
        ActionGraphEntity actionGraphEntity = new Builder(TopologyEntityDTO.newBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true) // should not be used
                            .setHotReplaceSupported(false)
                            .setHotRemoveSupported(true) // should not be used
                            .build())
                    .buildPartial())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true) // should not be used
                            .setHotReplaceSupported(false)
                            .setHotRemoveSupported(true)  // should not be used
                            .build())
                    .buildPartial())
            .buildPartial())
            .build();
        Assert.assertEquals(
            0,
            actionGraphEntity.asPartialEntity().getCommTypesWithHotReplaceCount());
    }

    /**
     * An ActionGraphEntity with sold commodities and one hot resize info set hot replace to true
     * should should have that commodity type as hot replace.
     */
    @Test
    public void testReplaceCommodityTypesFromHasOneHotReplace() {
        ActionGraphEntity actionGraphEntity = new Builder(TopologyEntityDTO.newBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(false) // should not be used
                            .setHotReplaceSupported(true)
                            .setHotRemoveSupported(false) // should not be used
                            .build())
                    .buildPartial())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true) // should not be used
                            .setHotReplaceSupported(false)
                            .setHotRemoveSupported(true)  // should not be used
                            .build())
                    .buildPartial())
            .buildPartial())
            .build();
        Assert.assertEquals(
            ImmutableSet.of(CommodityDTO.CommodityType.VMEM_VALUE),
            ImmutableSet.copyOf(actionGraphEntity.asPartialEntity().getCommTypesWithHotReplaceList()));
    }


    /**
     * An ActionGraphEntity with sold commodities and one hot resize info set hot replace to true
     * should should have that commodity type as hot replace.
     */
    @Test
    public void testReplaceCommodityTypesFromHasTwoHotReplace() {
        ActionGraphEntity actionGraphEntity = new Builder(TopologyEntityDTO.newBuilder()
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(false) // should not be used
                            .setHotReplaceSupported(true)
                            .setHotRemoveSupported(false) // should not be used
                            .build())
                    .buildPartial())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(false) // should not be used
                            .setHotReplaceSupported(true)
                            .setHotRemoveSupported(false) // should not be used
                            .build())
                    .buildPartial())
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE)
                            .build())
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true) // should not be used
                            .setHotReplaceSupported(false)
                            .setHotRemoveSupported(true)  // should not be used
                            .build())
                    .buildPartial())
            .buildPartial())
            .build();
        Assert.assertEquals(
            ImmutableSet.of(
                    CommodityDTO.CommodityType.VMEM_VALUE,
                    CommodityDTO.CommodityType.VCPU_VALUE),
            ImmutableSet.copyOf(actionGraphEntity.asPartialEntity().getCommTypesWithHotReplaceList()));
    }
}