package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.EXTENT_VALUE;
import static com.vmturbo.topology.processor.template.StorageEntityConstructor.COMMODITY_KEY_PREFIX;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtKey;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommodityBoughtValue;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySold;
import static com.vmturbo.topology.processor.template.TemplateConverterTestUtil.getCommoditySoldValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.Mockito;

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
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Unit tests for {@link StorageEntityConstructor}.
 */
public class StorageEntityConstructorTest {
    private double epsilon = 1e-5;

    private static final TemplateInfo ST_TEMPLATE_INFO = TemplateInfo.newBuilder()
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

    private static final Template ST_TEMPLATE = Template.newBuilder()
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
                .setType(EXTENT_VALUE)
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

    private final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(
        1L, TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(1).setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setAccesses(3)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(DATASTORE_VALUE).setKey("Storage::3")))),
        2L, TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(2).setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setAccesses(3)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(DATASTORE_VALUE).setKey("Storage::3")))),
        3L, TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(3).setEntityType(EntityType.STORAGE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setAccesses(1)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(DSPM_ACCESS_VALUE).setKey("PhysicalMachine::1")))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder().setAccesses(2)
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                    .setType(DSPM_ACCESS_VALUE).setKey("PhysicalMachine::2")))),
        4L, TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
            .setOid(4).setEntityType(EntityType.DISK_ARRAY_VALUE))
    );

    @Test
    public void testSTConvert() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(10L);

        final TopologyEntityDTO.Builder topologyEntityDTO = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(ST_TEMPLATE, topology, null, false,
                        identityProvider);

        // 6 commodities sold: storage latency, provisioned, amount, access and 2 DSPM_ACCESS
        assertEquals(6, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(200.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_AMOUNT_VALUE), epsilon);
        assertEquals(100.0, getCommoditySoldValue(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_ACCESS_VALUE), epsilon);
        // Verify that latency, provisioned capacity is not set
        assertFalse(getCommoditySold(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_LATENCY_VALUE).get().hasCapacity());
        assertFalse(getCommoditySold(topologyEntityDTO.getCommoditySoldListList(),
            CommodityType.STORAGE_PROVISIONED_VALUE).get().hasCapacity());

        // Verify DSPM_ACCESS commodity sold
        final List<CommoditySoldDTO> commSolds = topologyEntityDTO.getCommoditySoldListList().stream()
            .filter(commSold -> commSold.getCommodityType().getType() == DSPM_ACCESS_VALUE)
            .sorted((c1, c2) -> (int)(c1.getAccesses() - c2.getAccesses()))
            .collect(Collectors.toList());
        assertEquals(1, commSolds.get(0).getAccesses());
        assertEquals("PhysicalMachine::1", commSolds.get(0).getCommodityType().getKey());
        assertEquals(2, commSolds.get(1).getAccesses());
        assertEquals("PhysicalMachine::2", commSolds.get(1).getCommodityType().getKey());

        // Verify Extent commodity sold by disk array
        assertEquals(COMMODITY_KEY_PREFIX + EntityType.DISK_ARRAY.name() +
                COMMODITY_KEY_SEPARATOR + topologyEntityDTO.getOid(),
            getCommoditySold(topology.get(4L).getEntityBuilder().getCommoditySoldListList(),
                EXTENT_VALUE).get().getCommodityType().getKey());

        //Verify Extent commodity bought
        assertEquals(1, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        assertEquals(1.0, getCommodityBoughtValue(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
            EXTENT_VALUE), epsilon);
        assertEquals(COMMODITY_KEY_PREFIX + EntityType.DISK_ARRAY.name() +
                COMMODITY_KEY_SEPARATOR + topologyEntityDTO.getOid(),
            getCommodityBoughtKey(topologyEntityDTO.getCommoditiesBoughtFromProvidersList(),
                EXTENT_VALUE));
        assertTrue(topologyEntityDTO.getCommoditiesBoughtFromProviders(0).getMovable());
    }

    @Test
    public void testSTConvertWithConstraint() throws Exception {
        IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(1L);

        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder().setOid(10)
                .setEntityType(2)
                .addAllCommoditySoldList(stCommoditySold)
                .addAllCommoditiesBoughtFromProviders(stCommodityBoughtFromProvider);
        TopologyEntity.Builder topologyEntity = TopologyEntity.newBuilder(builder);

        final TopologyEntityDTO.Builder topologyEntityDTO = new StorageEntityConstructor()
                .createTopologyEntityFromTemplate(ST_TEMPLATE, topology, topologyEntity, false,
                        identityProvider);
        // 7 commodities sold: storage latency, provisioned, amount and access
        // storage cluster commodity and two dspm access commodities
        assertEquals(7, topologyEntityDTO.getCommoditySoldListCount());
        assertEquals(3,
                topologyEntityDTO.getCommoditySoldListList().stream().filter(
                        commoditySoldDTO -> !commoditySoldDTO.getCommodityType().getKey().isEmpty())
                        .count());
        assertEquals("123-extent", getCommodityBoughtKey(
                topologyEntityDTO.getCommoditiesBoughtFromProvidersList(), EXTENT_VALUE));
    }
}
