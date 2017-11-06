package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class VirtualMachineEntityConstructorTest {
    private final static TemplateInfo VM_TEMPLATE_INFO = TemplateInfo.newBuilder()
        .setName("test-VM-template")
        .setTemplateSpecId(1)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateField.newBuilder()
                .setName("numOfCpu")
                .setValue("2"))
            .addFields(TemplateField.newBuilder()
                .setName("cpuSpeed")
                .setValue("200"))
            .addFields(TemplateField.newBuilder()
                .setName("cpuConsumedFactor")
                .setValue("0.1"))
            .addFields(TemplateField.newBuilder()
                .setName("memorySize")
                .setValue("100"))
            .addFields(TemplateField.newBuilder()
                .setName("memoryConsumedFactor")
                .setValue("0.1"))
            .addFields(TemplateField.newBuilder()
                .setName("ioThroughput")
                .setValue("300"))
            .addFields(TemplateField.newBuilder()
                .setName("networkThroughput")
                .setValue("400")))
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Storage))
            .addFields(TemplateField.newBuilder()
                .setName("diskIops")
                .setValue("300"))
            .addFields(TemplateField.newBuilder()
                .setName("diskSize")
                .setValue("300"))
            .addFields(TemplateField.newBuilder()
                .setName("diskConsumedFactor")
                .setValue("0.1")))
        .build();

    private final static Template VM_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(VM_TEMPLATE_INFO)
        .build();

    @Test
    public void testVMConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO topologyEntityDTO =
            new VirtualMachineEntityConstructor().createTopologyEntityFromTemplate(VM_TEMPLATE, builder);
        assertEquals(3, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(2, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(400.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VCPU_VALUE), 0.00000001);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VMEM_VALUE), 0.00000001);
        assertEquals(300.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.VSTORAGE_VALUE), 0.00000001);
        assertEquals(10.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_VALUE), 0.00000001);
        assertEquals(100.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.MEM_PROVISIONED_VALUE), 0.00000001);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_ACCESS_VALUE), 0.00000001);
        assertEquals(30.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.STORAGE_AMOUNT_VALUE), 0.00000001);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.IO_THROUGHPUT_VALUE), 0.00000001);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.NET_THROUGHPUT_VALUE), 0.00000001);
        assertEquals(40.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.CPU_VALUE), 0.00000001);
        assertFalse(topologyEntityDTO.getConsumerPolicy().getShopsTogether());
    }
}
