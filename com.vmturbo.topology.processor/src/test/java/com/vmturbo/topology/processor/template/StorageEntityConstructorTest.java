package com.vmturbo.topology.processor.template;

import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtKey;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

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

    private static Set<CommoditySoldDTO> stCommoditySold = Sets.newHashSet(
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.DSPM_ACCESS_VALUE)
                .setKey("123-dspm"))
            .setUsed(1)
            .build(),
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.DSPM_ACCESS_VALUE)
                .setKey("456-dspm"))
            .setUsed(1)
            .build(),
        CommoditySoldDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.STORAGE_CLUSTER_VALUE)
                .setKey("123-storage-cluster"))
            .setUsed(1)
            .build()
    );
    private static List<CommodityBoughtDTO> commodityBoughtFromDiskArray = Lists.newArrayList(
        CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.EXTENT_VALUE)
                .setKey("123-extent"))
            .setUsed(1)
            .build()
    );

    private static CommoditiesBoughtFromProvider commodityBoughtFromProviderDiskArray =
        CommoditiesBoughtFromProvider.newBuilder()
            .addAllCommodityBought(commodityBoughtFromDiskArray)
            .setProviderId(456)
            .setProviderEntityType(EntityType.DISK_ARRAY_VALUE)
            .build();

    public static List<CommoditiesBoughtFromProvider> stCommodityBoughtFromProvider =
        Stream.of(commodityBoughtFromProviderDiskArray).collect(Collectors.toList());

    private final Map<Long, TopologyEntity.Builder> topology = Collections.emptyMap();

    @Test
    public void testSTConvert() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new StorageEntityConstructor().createTopologyEntityFromTemplate(ST_TEMPLATE, builder,
                topology, null);

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

    @Test
    public void testSTConvertWithConstraint() {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_VALUE)
            .setOid(1);
        final TopologyEntityDTO.Builder topologyEntityDTO =
            new StorageEntityConstructor().createTopologyEntityFromTemplate(ST_TEMPLATE, builder, topology,
                TopologyEntityDTO.newBuilder()
                    .setOid(1)
                    .setEntityType(2)
                    .addAllCommoditySoldList(stCommoditySold)
                    .addAllCommoditiesBoughtFromProviders(stCommodityBoughtFromProvider)
                    .build());
        assertEquals(5, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(3, topologyEntityDTO.getCommoditySoldListList().stream()
            .filter(commoditySoldDTO ->
                !commoditySoldDTO.getCommodityType().getKey().isEmpty())
            .count());
        assertEquals("123-extent", getCommodityBoughtKey(
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), CommodityType.EXTENT_VALUE));
    }
}
