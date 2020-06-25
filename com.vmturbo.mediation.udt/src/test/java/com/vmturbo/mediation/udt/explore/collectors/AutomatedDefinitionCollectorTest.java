package com.vmturbo.mediation.udt.explore.collectors;

import static com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.mediation.udt.explore.DataProvider;
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
        Long definitionId = 1000L;
        String tag = "Region";
        EntityType entityType = EntityType.SERVICE;
        EntityType connectedEntityType = EntityType.VIRTUAL_MACHINE;
        AutomatedEntityDefinition definition = AutomatedEntityDefinition.newBuilder()
                .setNamingPrefix("udt")
                .setEntityType(entityType)
                .setConnectedEntityType(connectedEntityType)
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                        .setTagKey(tag)
                        .build())
                .build();
        AutomatedDefinitionCollector collector = new AutomatedDefinitionCollector(definitionId, definition);
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
        Mockito.when(dataProvider.getEntitiesByTag(tag, connectedEntityType))
                .thenReturn(Sets.newSet(vm1, vm2, vm3));
        Set<UdtEntity> set = collector.collectEntities(dataProvider);
        Assert.assertEquals(2, set.size());
        UdtEntity usRegionEntity = set.stream().filter(udt -> udt.getChildren().size() == 2).findFirst().orElse(null);
        Assert.assertNotNull(usRegionEntity);
        Assert.assertEquals("udt_Region_us", usRegionEntity.getName());
        Assert.assertEquals("0474bc8c48a6b0bfc77bda29c045075e9451c384", usRegionEntity.getId());
        Assert.assertEquals(EntityType.SERVICE, usRegionEntity.getEntityType());
    }

}
