package com.vmturbo.mediation.udt.explore.collectors;

import static com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.search.Search.TaggedEntities;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link AutomatedDefinitionCollector}.
 */
public class AutomatedDefinitionCollectorTest {

    /**
     * The method tests that the collector correctly generates ID for a UDT entity.
     */
    @Test
    public void testGenerateId() {
        AutomatedDefinitionCollector collector
                = new AutomatedDefinitionCollector(1000L, AutomatedEntityDefinition.newBuilder().build());
        String name = "some-object-name";
        String id = collector.generateUdtEntityId(name);
        Assert.assertEquals("1d3f1f6ed0961903b2edac41d3f99921c176b900", id);
    }

    /**
     * The method tests that the collector correctly creates UDT entities from VMs with defined tag.
     */
    @Test
    public void testCollectEntities() {
        String tag = "Region";
        EntityType connectedEntityType = EntityType.VIRTUAL_MACHINE;
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        TopologyEntityDTO vm1 = TestUtils.createTopologyDto(1L, "vm-1", EntityType.VIRTUAL_MACHINE)
                .toBuilder()
                .setTags(Tags.newBuilder()
                        .putTags(tag, TagValuesDTO.newBuilder().addValues("us").build()).build())
                .build();
        TopologyEntityDTO vm2 = TestUtils.createTopologyDto(2L, "vm-2", EntityType.VIRTUAL_MACHINE)
                .toBuilder()
                .setTags(Tags.newBuilder()
                        .putTags(tag, TagValuesDTO.newBuilder().addValues("us").build()).build())
                .build();
        TopologyEntityDTO vm3 = TestUtils.createTopologyDto(3L, "vm-3", EntityType.VIRTUAL_MACHINE)
                .toBuilder()
                .setTags(Tags.newBuilder()
                        .putTags(tag, TagValuesDTO.newBuilder().addValues("eu").build()).build())
                .build();

        Map<String, TaggedEntities> map = Maps.newHashMap();
        map.put("us", TaggedEntities.newBuilder().addOid(vm1.getOid()).addOid(vm2.getOid()).build());
        map.put("eu", TaggedEntities.newBuilder().addOid(vm3.getOid()).build());
        Mockito.when(dataProvider.retrieveTagValues(tag, connectedEntityType))
                .thenReturn(map);
        AutomatedEntityDefinition definition = AutomatedEntityDefinition.newBuilder()
                .setNamingPrefix("udt")
                .setEntityType(EntityType.SERVICE)
                .setConnectedEntityType(connectedEntityType)
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                        .setTagKey(tag)
                        .build())
                .build();
        AutomatedDefinitionCollector collector = new AutomatedDefinitionCollector(1000L, definition);
        Set<UdtEntity> set = collector.collectEntities(dataProvider);
        Assert.assertEquals(2, set.size());
        UdtEntity usRegionEntity = set.stream().filter(udt -> udt.getChildren().size() == 2).findFirst().orElse(null);
        Assert.assertNotNull(usRegionEntity);
        Assert.assertEquals("udt_Region_us", usRegionEntity.getName());
        Assert.assertEquals("0474bc8c48a6b0bfc77bda29c045075e9451c384", usRegionEntity.getId());
        Assert.assertEquals(EntityType.SERVICE, usRegionEntity.getEntityType());
    }

    /**
     * The method tests the case when there is a tag with multiple values in auto TDD .
     * We should handle it and create a user-defined entity for each value.
     */
    @Test
    public void testMultipleTagValues() {
        String tag = "Region";
        EntityType connectedEntityType = EntityType.VIRTUAL_MACHINE;
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        TopologyEntityDTO vm1 = TestUtils.createTopologyDto(1L, "vm-1", EntityType.VIRTUAL_MACHINE)
                .toBuilder()
                .setTags(Tags.newBuilder()
                        .putTags(tag, TagValuesDTO.newBuilder()
                                .addValues("us:1") // tag 1
                                .addValues("us:2") // tag 2
                                .build()).build())
                .build();
        Map<String, TaggedEntities> map = Maps.newHashMap();
        map.put("us:1", TaggedEntities.newBuilder().addOid(vm1.getOid()).build());
        map.put("us:2", TaggedEntities.newBuilder().addOid(vm1.getOid()).build());

        Mockito.when(dataProvider.retrieveTagValues(tag, connectedEntityType)).thenReturn(map);
        AutomatedEntityDefinition definition = AutomatedEntityDefinition.newBuilder()
                .setNamingPrefix("udt")
                .setEntityType(EntityType.SERVICE)
                .setConnectedEntityType(connectedEntityType)
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                        .setTagKey(tag)
                        .build())
                .build();
        AutomatedDefinitionCollector collector = new AutomatedDefinitionCollector(1000L, definition);
        Set<UdtEntity> set = collector.collectEntities(dataProvider);
        Assert.assertEquals(2, set.size());
        List<UdtEntity> udtList = new ArrayList<>(set);
        MatcherAssert.assertThat(udtList.stream().map(UdtEntity::getName).collect(Collectors.toSet()),
                containsInAnyOrder("udt_Region_us:1", "udt_Region_us:2"));
        UdtChildEntity child1 = udtList.get(0).getChildren().iterator().next();
        UdtChildEntity child2 = udtList.get(1).getChildren().iterator().next();
        Assert.assertEquals(child1.getOid(), child2.getOid());
        Assert.assertEquals(child1.getEntityType(), child2.getEntityType());
    }

}
