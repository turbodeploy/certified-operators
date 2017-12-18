package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtKey;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.util.Lists;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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

    private static Set<CommoditySoldDTO> pmCommoditySold = Sets.newHashSet(
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.CLUSTER_VALUE)
                .setKey("123-cluster"))
            .setUsed(1)
            .build(),
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.DATASTORE_VALUE)
                .setKey("123-data-store"))
            .setUsed(1)
            .build(),
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.DATASTORE_VALUE)
                .setKey("456-data-store"))
            .setUsed(1)
            .build(),
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.NETWORK_VALUE)
                .setKey("123-network"))
            .setUsed(1)
            .build()
    );
    private static List<CommodityBoughtDTO> pmCommodityBoughtFromDataCenter = Lists.newArrayList(
        CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.DATACENTER_VALUE)
                .setKey("123-data-center"))
            .setUsed(1)
            .build()
    );

    private static CommoditiesBoughtFromProvider commodityBoughtFromProviderDataCenter =
        CommoditiesBoughtFromProvider.newBuilder()
            .addAllCommodityBought(pmCommodityBoughtFromDataCenter)
            .setProviderId(123)
            .setProviderEntityType(EntityType.DATACENTER_VALUE)
            .build();

    public static List<CommoditiesBoughtFromProvider> pmCommodityBoughtFromProvider =
        Stream.of(commodityBoughtFromProviderDataCenter).collect(Collectors.toList());



    @Test
    public void testPMConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new PhysicalMachineEntityConstructor().createTopologyEntityFromTemplate(PM_TEMPLATE,
                builder, null);
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

    @Test
    public void testPMConvertWithConstraint() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new PhysicalMachineEntityConstructor().createTopologyEntityFromTemplate(PM_TEMPLATE,
                builder,
                TopologyEntityDTO.newBuilder()
                    .setOid(1)
                    .setEntityType(14)
                    .addAllCommoditySoldList(pmCommoditySold)
                    .addAllCommoditiesBoughtFromProviders(pmCommodityBoughtFromProvider)
                    .build());
        assertEquals(8, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(4, topologyEntityDTO.getCommoditySoldListList().stream()
            .filter(commoditySoldDTO ->
                !commoditySoldDTO.getCommodityType().getKey().isEmpty())
            .count());
        assertEquals("123-data-center", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.DATACENTER_VALUE));
    }
}
