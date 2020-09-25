package com.vmturbo.mediation.udt.explore;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.mediation.udt.explore.collectors.AutomatedDefinitionCollector;
import com.vmturbo.mediation.udt.explore.collectors.ManualDefinitionCollector;
import com.vmturbo.mediation.udt.explore.collectors.UdtCollector;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link UdtProbeExplorer}.
 */
public class UdtProbeExplorerTest {

    /**
     * Tests that UdtProbeExplorer creates correct collectors: manual/automated.
     */
    @Test
    public void testCreateCollector() {
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        UdtProbeExplorer explorer = new UdtProbeExplorer(dataProvider, Executors.newSingleThreadExecutor());
        TopologyDataDefinition tddManual = TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder().buildPartial())
                .build();
        TopologyDataDefinition tddAutomated = TopologyDataDefinition.newBuilder()
                .setAutomatedEntityDefinition(AutomatedEntityDefinition.newBuilder().build())
                .build();
        UdtCollector manCollector = explorer.createCollector(1000L, tddManual);
        UdtCollector autoCollector = explorer.createCollector(2000L, tddAutomated);

        Assert.assertNotNull(manCollector);
        Assert.assertNotNull(autoCollector);
        Assert.assertTrue(manCollector instanceof ManualDefinitionCollector);
        Assert.assertTrue(autoCollector instanceof AutomatedDefinitionCollector);
    }

    /**
     * Tests that UdtProbeExplorer correctly collects UDT entities. Here is an example for static members.
     */
    @Test
    public void testExploringEntities() {
        long childOid  = 8080L;
        long udtId = 1000L;
        EntityType udtType = EntityType.SERVICE;
        EntityType udtChildType = EntityType.VIRTUAL_MACHINE;
        TopologyDataDefinition tddManual = TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                        .setEntityType(udtType)
                        .setEntityName("svc-0")
                        .addAssociatedEntities(AssociatedEntitySelectionCriteria.newBuilder()
                                .setConnectedEntityType(udtChildType)
                                .setStaticAssociatedEntities(StaticMembers.newBuilder()
                                        .addMembersByType(StaticMembersByType.newBuilder()
                                                .addMembers(childOid)
                                                .setType(MemberType.newBuilder()
                                                        .setEntity(udtChildType.getNumber())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .buildPartial())
                .build();
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        Mockito.when(dataProvider.getTopologyDataDefinitions())
                .thenReturn(Collections.singletonMap(udtId, tddManual));

        UdtProbeExplorer explorer = new UdtProbeExplorer(dataProvider, Executors.newSingleThreadExecutor());
        Set<UdtEntity> entities = explorer.exploreDataDefinition();

        Assert.assertFalse(entities.isEmpty());
        UdtEntity udtEntity = entities.iterator().next();

        Assert.assertEquals(udtType, udtEntity.getEntityType());
        Assert.assertEquals("svc-0", udtEntity.getName());
        Assert.assertEquals(udtId, Long.parseLong(udtEntity.getId()));

        Assert.assertFalse(udtEntity.getChildren().isEmpty());

        UdtChildEntity udtChild = udtEntity.getChildren().iterator().next();

        Assert.assertEquals(childOid, udtChild.getOid());
        Assert.assertEquals(udtChildType, udtChild.getEntityType());

    }

}
