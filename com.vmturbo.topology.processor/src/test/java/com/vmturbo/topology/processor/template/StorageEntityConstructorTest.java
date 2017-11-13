package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
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

public class StorageEntityConstructorTest {
    private double epsilon = 1e-5;

    private final static TemplateInfo ST_TEMPLATE_INFO = TemplateInfo.newBuilder()
        .setName("test-st-template")
        .setTemplateSpecId(3)
        .setEntityType(EntityType.STORAGE_VALUE)
        .addResources(TemplateResource.newBuilder()
            .setCategory(ResourcesCategory.newBuilder()
                .setName(ResourcesCategoryName.Storage))
            .addFields(TemplateField.newBuilder()
                .setName("diskIops")
                .setValue("100"))
            .addFields(TemplateField.newBuilder()
                .setName("diskSize")
                .setValue("200")))
        .build();

    private final static Template ST_TEMPLATE = Template.newBuilder()
        .setId(123)
        .setTemplateInfo(ST_TEMPLATE_INFO)
        .build();

    @Test
    public void testSTConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setOid(1);
        final TopologyEntityDTO topologyEntityDTO =
            new StorageEntityConstructor().createTopologyEntityFromTemplate(ST_TEMPLATE, builder);

        assertEquals(2, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(1, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(200.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        assertEquals(1.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            CommodityType.EXTENT_VALUE), epsilon);
        assertTrue(topologyEntityDTO.getCommoditiesBoughtFromProviders(0).getMovable());
    }
}
