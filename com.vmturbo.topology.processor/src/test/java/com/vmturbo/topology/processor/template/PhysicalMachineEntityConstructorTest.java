package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

public class PhysicalMachineEntityConstructorTest {
    private double epsilon = 1e-5;

    private final static TemplateInfo PM_TEMPLATE_INFO = TemplateInfo.newBuilder()
        .setName("test-PM-template")
        .setTemplateSpecId(2)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Compute))
            .addFields(TemplateField.newBuilder()
                .setName("numOfCores")
                .setValue("10"))
            .addFields(TemplateField.newBuilder()
                .setName("cpuSpeed")
                .setValue("20"))
            .addFields(TemplateField.newBuilder()
                .setName("memorySize")
                .setValue("100"))
            .addFields(TemplateField.newBuilder()
                .setName("ioThroughputSize")
                .setValue("30"))
            .addFields(TemplateField.newBuilder()
                .setName("networkThroughputSize")
                .setValue("40")))
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Infrastructure))
            .addFields(TemplateField.newBuilder()
                .setName("powerSize")
                .setValue("200"))
            .addFields(TemplateField.newBuilder()
                .setName("spaceSize")
                .setValue("300"))
            .addFields(TemplateField.newBuilder()
                .setName("coolingSize")
                .setValue("400")))
        .build();

    private final static Template PM_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(PM_TEMPLATE_INFO)
        .build();

    @Test
    public void testPMConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO topologyEntityDTO =
            new PhysicalMachineEntityConstructor().createTopologyEntityFromTemplate(PM_TEMPLATE, builder);
        assertEquals(4, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(1, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(200.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.CPU_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.MEM_VALUE), epsilon);
        assertEquals(30.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.IO_THROUGHPUT_VALUE), epsilon);
        assertEquals(40.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.NET_THROUGHPUT_VALUE), epsilon);

        assertEquals(200.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.POWER_VALUE), epsilon);
        assertEquals(300.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.COOLING_VALUE), epsilon);
        assertEquals(400.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.SPACE_VALUE), epsilon);
        assertTrue(topologyEntityDTO.getCommoditiesBoughtFromProviders(0).getMovable());
        assertFalse(topologyEntityDTO.getAnalysisSettings().getShopTogether());
    }
}
