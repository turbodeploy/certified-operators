package com.vmturbo.action.orchestrator.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;

/**
 * Unit tests for {@link ActionRealtimeTopology}, and related classes.
 */
public class ActionRealtimeTopologyTest {

    private static final long TARGET_ID = 123L;

    private static final CommoditySoldDTO VMEM_SOLD = CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(UICommodityType.VMEM.typeNumber()))
            .setCapacity(123)
            .setHotResizeInfo(HotResizeInfo.newBuilder()
                    .setHotReplaceSupported(true))
            .build();

    private static final TopologyEntityDTO CONSUMER = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.APPLICATION.typeNumber())
            .setDisplayName("consumer")
            .setOid(666L)
            .build();

    private static final TopologyEntityDTO PROVIDER = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .setDisplayName("provider")
            .setOid(888L)
            .build();

    private static final TopologyEntityDTO CONNECTED_TO = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.VIRTUAL_DATACENTER.typeNumber())
            .setDisplayName("connection")
            .setOid(1777L)
            .build();

    private static final TopologyEntityDTO OWNS = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
            .setDisplayName("owns")
            .setOid(17777L)
            .build();

    private static final TopologyEntityDTO ENTITY = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName("foo")
            .setOid(7L)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(TARGET_ID, PerTargetEntityInformation.getDefaultInstance())))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setNumCpus(10)))
            .setEntityState(EntityState.POWERED_OFF)
            .setTags(Tags.newBuilder()
                    .putTags("tag", TagValuesDTO.newBuilder()
                            .addValues("val")
                            .build()))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(PROVIDER.getOid())
                    .setProviderEntityType(PROVIDER.getEntityType()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(CONNECTED_TO.getOid())
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityType(CONNECTED_TO.getEntityType()))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(OWNS.getOid())
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .setConnectedEntityType(OWNS.getEntityType()))
            .addCommoditySoldList(VMEM_SOLD)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(UICommodityType.STORAGE_ACCESS.typeNumber()))
                    .setCapacity(321))
            .build();

    private static final TopologyEntityDTO VSAN = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.STORAGE.typeNumber())
            .setDisplayName("vsan_storage")
            .setOid(77L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setStorage(
                    StorageInfo.newBuilder().setStorageType(StorageType.VSAN)))
            .build();

    private static final TopologyEntityDTO HOST_FOR_VSAN = TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .setDisplayName("host_for_vsan")
            .setOid(887L)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.getDefaultInstance()))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setPhysicalMachine(PhysicalMachineInfo.newBuilder().setNumCpus(2)))
            .build();

    ActionGraphEntity graphEntity;
    ActionGraphEntity hostForVSANEntity;

    /**
     * Common setup code before every test.
     */
    @Before
    public void setup() {
        ActionGraphEntity.Builder providerBldr = new ActionGraphEntity.Builder(PROVIDER);
        ActionGraphEntity.Builder connectedToBldr = new ActionGraphEntity.Builder(CONNECTED_TO);
        ActionGraphEntity.Builder ownsBldr = new ActionGraphEntity.Builder(OWNS);
        ActionGraphEntity.Builder graphBldr = new ActionGraphEntity.Builder(ENTITY);
        graphBldr.addProvider(providerBldr);
        graphBldr.addConsumer(new ActionGraphEntity.Builder(CONSUMER));
        graphBldr.addOutboundAssociation(connectedToBldr);
        graphBldr.addOwnedEntity(ownsBldr);

        graphEntity = graphBldr.build();

        ActionGraphEntity.Builder hostForVSANBldr = new ActionGraphEntity.Builder(HOST_FOR_VSAN);
        hostForVSANBldr.addConsumer(new ActionGraphEntity.Builder(VSAN));
        hostForVSANEntity = hostForVSANBldr.build();
    }

    /**
     * Test conversion of graph entity to {@link ActionPartialEntity}.
     */
    @Test
    public void testGraphEntityToAction() {
        final ActionPartialEntity actionEntity = graphEntity.asPartialEntity();
        assertThat(actionEntity.getOid(), is(graphEntity.getOid()));
        assertThat(actionEntity.getDisplayName(), is(graphEntity.getDisplayName()));
        assertThat(actionEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(actionEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(actionEntity.getCommTypesWithHotReplaceList(), contains(UICommodityType.VMEM.typeNumber()));
    }

    /**
     * Test conversion of a host with a vSAN among its consumers.
     */
    @Test
    public void testHostForVSANToAction()   {
        final ActionPartialEntity actionEntity = hostForVSANEntity.asPartialEntity();
        assertEquals(hostForVSANEntity.getOid(), actionEntity.getOid());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, actionEntity.getEntityType());
        assertEquals(1, actionEntity.getConnectedEntitiesCount());

        ConnectedEntity connected = actionEntity.getConnectedEntitiesList().get(0);
        assertTrue(connected.hasConnectedEntityId() && connected.hasConnectedEntityType());
        assertEquals(VSAN.getOid(), connected.getConnectedEntityId());
        assertEquals(EntityType.STORAGE_VALUE, connected.getConnectedEntityType());
    }
}
