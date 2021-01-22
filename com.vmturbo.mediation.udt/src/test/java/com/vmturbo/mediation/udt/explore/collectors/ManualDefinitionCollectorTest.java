package com.vmturbo.mediation.udt.explore.collectors;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.DynamicConnectionFilters;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link ManualDefinitionCollector}.
 */
public class ManualDefinitionCollectorTest {
    private static final Long DEFINITION_ID = 1000L;

    /**
     * The method test that the collector correctly handles definition without associated entities.
     */
    @Test
    public void testCollectEntities() {
        ManualEntityDefinition definition = ManualEntityDefinition
                .newBuilder()
                .setEntityName("UDT-Service")
                .setEntityType(EntityType.SERVICE)
                .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder().build())
                .build();
        ManualDefinitionCollector collector = new ManualDefinitionCollector(DEFINITION_ID, definition);
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Set<UdtEntity> udtEntities = collector.collectEntities(dataProvider);
        Assert.assertFalse(udtEntities.isEmpty());
        UdtEntity udtEntity = udtEntities.iterator().next();
        Assert.assertEquals(String.valueOf(DEFINITION_ID), udtEntity.getId());
        Assert.assertEquals(EntityType.SERVICE, udtEntity.getEntityType());
        Assert.assertEquals("UDT-Service", udtEntity.getName());
    }

    /**
     * The method test that the collector correctly handles invalid definitions.
     */
    @Test
    public void testCollectEntitiesIfDefinitionInvalid() {
        ManualEntityDefinition definition = ManualEntityDefinition
                .newBuilder()
                .build();
        ManualDefinitionCollector collector = new ManualDefinitionCollector(DEFINITION_ID, definition);
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Set<UdtEntity> udtEntities = collector.collectEntities(dataProvider);
        Assert.assertTrue(udtEntities.isEmpty());
    }

    /**
     * The method test that the collector correctly handles definition with static members.
     */
    @Test
    public void testCollectEntitiesStaticMembers() {
        ManualEntityDefinition definition = ManualEntityDefinition
                .newBuilder()
                .setEntityName("UDT-Service")
                .setEntityType(EntityType.SERVICE)
                .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE)
                        .setStaticAssociatedEntities(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .addMembers(100L)
                                        .addMembers(200L)
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.VIRTUAL_MACHINE.getNumber())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        ManualDefinitionCollector collector = new ManualDefinitionCollector(DEFINITION_ID, definition);
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Set<UdtEntity> udtEntities = collector.collectEntities(dataProvider);
        Assert.assertFalse(udtEntities.isEmpty());
        UdtEntity udtEntity = udtEntities.iterator().next();
        Set<UdtChildEntity> childs = udtEntity.getChildren();
        Assert.assertEquals(2, childs.size());
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE, childs.iterator().next().getEntityType());
    }

    /**
     * The method test that the collector correctly handles definition with group members.
     */
    @Test
    public void testCollectEntitiesGroupMembers() {
        EntityType entityType = EntityType.VIRTUAL_MACHINE;
        GroupID groupID = GroupID.newBuilder().setId(333L).build();
        ManualEntityDefinition definition = ManualEntityDefinition
                .newBuilder()
                .setEntityName("UDT-Service")
                .setEntityType(EntityType.SERVICE)
                .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                        .setConnectedEntityType(entityType)
                        .setAssociatedGroup(groupID)
                        .build())
                .build();
        ManualDefinitionCollector collector = new ManualDefinitionCollector(DEFINITION_ID, definition);
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Mockito.when(dataProvider.getGroupMembersIds(groupID))
                .thenReturn(Sets.newSet(1000L, 2000L, 3000L));
        Set<UdtEntity> udtEntities = collector.collectEntities(dataProvider);
        Assert.assertFalse(udtEntities.isEmpty());
        UdtEntity udtEntity = udtEntities.iterator().next();
        Set<UdtChildEntity> childs = udtEntity.getChildren();
        Assert.assertEquals(3, childs.size());
        Assert.assertEquals(entityType, childs.iterator().next().getEntityType());
    }

    /**
     * The method test that the collector correctly handles definition with filtered members.
     */
    @Test
    public void testCollectEntitiesFiltersMembers() {
        FilterSpecs filterSpecs = FilterSpecs.newBuilder()
                .setExpressionType("exp_type")
                .setFilterType("f_type")
                .setExpressionValue("exp_value")
                .build();
        DynamicConnectionFilters dynamicConnectionFilters = DynamicConnectionFilters.newBuilder()
                .addSearchParameters(SearchParameters.newBuilder()
                        .setSourceFilterSpecs(filterSpecs)
                        .build())
                .build();
        ManualEntityDefinition definition = ManualEntityDefinition
                .newBuilder()
                .setEntityName("UDT-Service")
                .setEntityType(EntityType.SERVICE)
                .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                        .setDynamicConnectionFilters(dynamicConnectionFilters)
                        .setConnectedEntityType(EntityType.APPLICATION_COMPONENT)
                        .build())
                .build();
        ManualDefinitionCollector collector = new ManualDefinitionCollector(DEFINITION_ID, definition);
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Mockito.when(dataProvider.searchEntities(dynamicConnectionFilters.getSearchParametersList()))
                .thenReturn(Sets.newSet(
                        TestUtils.createTopologyDto(1L, "some-name-a", EntityType.APPLICATION_COMPONENT),
                        TestUtils.createTopologyDto(2L, "some-name-b", EntityType.APPLICATION_COMPONENT)
                ));
        Set<UdtEntity> udtEntities = collector.collectEntities(dataProvider);
        Assert.assertFalse(udtEntities.isEmpty());
        UdtEntity udtEntity = udtEntities.iterator().next();
        Assert.assertEquals(2, udtEntity.getChildren().size());
        UdtChildEntity udtChildEntity = udtEntity.getChildren().iterator().next();
        Assert.assertEquals(EntityType.APPLICATION_COMPONENT, udtChildEntity.getEntityType());
    }

}
