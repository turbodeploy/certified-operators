package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtKey;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

public class VirtualMachineEntityConstructorTest {
    private double epsilon = 1e-5;

    private final static Template VM_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(TemplateConverterTestUtil.VM_TEMPLATE_INFO)
        .build();

    private final Map<Long, TopologyEntity.Builder> topology = Collections.emptyMap();

    @Test
    public void testVMConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new VirtualMachineEntityConstructor().createTopologyEntityFromTemplate(VM_TEMPLATE, builder,
                topology, null);
        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VMEM_VALUE), epsilon);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VSTORAGE_VALUE), epsilon);
        assertEquals(10.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_VALUE), epsilon);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_PROVISIONED_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        assertEquals(30.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_PROVISIONED_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.IO_THROUGHPUT_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.NET_THROUGHPUT_VALUE), epsilon);
        assertEquals(40.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_PROVISIONED_VALUE), epsilon);
    }

    @Test
    public void testVMConvertWithConstraints() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new VirtualMachineEntityConstructor().createTopologyEntityFromTemplate(VM_TEMPLATE, builder, topology,
                TopologyEntityDTO.newBuilder()
                    .setOid(1)
                    .setEntityType(10)
                    .addAllCommoditySoldList(TemplateConverterTestUtil.VM_COMMODITY_SOLD)
                    .addAllCommoditiesBoughtFromProviders(TemplateConverterTestUtil.VM_COMMODITY_BOUGHT_FROM_PROVIDER)
                    .build());

        assertEquals(4, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(1, topologyEntityDTO.getCommoditySoldListList().stream()
            .filter(commoditySoldDTO ->
                commoditySoldDTO.getCommodityType().getType() == CommodityType.APPLICATION_VALUE)
            .count());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals("123-network", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.NETWORK_VALUE));
        assertEquals("123-datastore", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.DATASTORE_VALUE));
        assertEquals("123-data-center", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.DATACENTER_VALUE));
        assertEquals("123-cluster", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.CLUSTER_VALUE));
        assertEquals("123-storage-cluster", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.STORAGE_CLUSTER_VALUE));
        assertEquals("123-dspm-access", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.DSPM_ACCESS_VALUE));
        assertEquals("123-extent", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.EXTENT_VALUE));
    }

    @Test
    public void testReservationVMConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
                new VirtualMachineEntityConstructor(true)
                        .createTopologyEntityFromTemplate(VM_TEMPLATE, builder, topology,
                            null);
        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VCPU_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VMEM_VALUE), epsilon);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
                CommodityType.VSTORAGE_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.MEM_VALUE), epsilon);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.MEM_PROVISIONED_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.STORAGE_PROVISIONED_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.IO_THROUGHPUT_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.NET_THROUGHPUT_VALUE), epsilon);
        assertEquals(0.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                CommodityType.CPU_PROVISIONED_VALUE), epsilon);
    }
}
