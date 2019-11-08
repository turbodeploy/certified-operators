package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.TypeSpecificPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;

public class PartialEntityConverterTest {

    private static final long TARGET_ID = 123L;

    private PartialEntityConverter converter = new PartialEntityConverter();

    private static final CommoditySoldDTO VMEM_SOLD = CommoditySoldDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder()
            .setType(UICommodityType.VMEM.typeNumber()))
        .setCapacity(123)
        .build();

    private static final TopologyEntityDTO PROVIDER = TopologyEntityDTO.newBuilder()
        .setEntityType(UIEntityType.PHYSICAL_MACHINE.typeNumber())
        .setDisplayName("provider")
        .setOid(888L)
        .build();

    private static final TopologyEntityDTO CONNECTED_TO = TopologyEntityDTO.newBuilder()
        .setEntityType(UIEntityType.VIRTUAL_DATACENTER.typeNumber())
        .setDisplayName("connection")
        .setOid(1777L)
        .build();

    private static final TopologyEntityDTO OWNS = TopologyEntityDTO.newBuilder()
        .setEntityType(UIEntityType.VIRTUAL_VOLUME.typeNumber())
        .setDisplayName("owns")
        .setOid(17777L)
        .build();

    private static final TopologyEntityDTO ENTITY = TopologyEntityDTO.newBuilder()
        .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
        .setDisplayName("foo")
        .setOid(7L)
        .setEnvironmentType(EnvironmentType.CLOUD)
        .setOrigin(Origin.newBuilder()
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                .addDiscoveringTargetIds(TARGET_ID)))
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

    private RepoGraphEntity graphEntity;

    @Before
    public void setup() {
        RepoGraphEntity.Builder providerBldr = RepoGraphEntity.newBuilder(PROVIDER);
        RepoGraphEntity.Builder connectedToBldr = RepoGraphEntity.newBuilder(CONNECTED_TO);
        RepoGraphEntity.Builder ownsBldr = RepoGraphEntity.newBuilder(OWNS);
        RepoGraphEntity.Builder graphBldr = RepoGraphEntity.newBuilder(ENTITY);
        graphBldr.addProvider(providerBldr);
        graphBldr.addOutboundAssociation(connectedToBldr);
        graphBldr.addOwnedEntity(ownsBldr);

        graphEntity = graphBldr.build();
    }

    @Test
    public void testTopoEntityToMinimal() {
        final MinimalEntity minEntity = converter.createPartialEntity(ENTITY, Type.MINIMAL).getMinimal();
        assertThat(minEntity.getOid(), is(ENTITY.getOid()));
        assertThat(minEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(minEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(minEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(minEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
    }

    @Test
    public void testTopoEntityToAction() {
        final ActionPartialEntity actionEntity = converter.createPartialEntity(ENTITY, Type.ACTION).getAction();
        assertThat(actionEntity.getOid(), is(ENTITY.getOid()));
        assertThat(actionEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(actionEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(actionEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(actionEntity.getCommoditySoldList(), contains(VMEM_SOLD));
    }

    @Test
    public void testTopoEntityToApi() {
        final ApiPartialEntity apiEntity = converter.createPartialEntity(ENTITY, Type.API).getApi();
        assertThat(apiEntity.getOid(), is(ENTITY.getOid()));
        assertThat(apiEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(apiEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(apiEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(apiEntity.getEntityState(), is(ENTITY.getEntityState()));
        assertThat(apiEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
        assertThat(apiEntity.getTags(), is(ENTITY.getTags()));
        assertThat(apiEntity.getProvidersList(), contains(RelatedEntity.newBuilder()
            .setEntityType(PROVIDER.getEntityType())
            .setOid(PROVIDER.getOid())
            .build()));
        assertThat(apiEntity.getConnectedToList(), contains(
            RelatedEntity.newBuilder()
                .setEntityType(CONNECTED_TO.getEntityType())
                .setOid(CONNECTED_TO.getOid())
                .build(),
            RelatedEntity.newBuilder()
                .setEntityType(OWNS.getEntityType())
                .setOid(OWNS.getOid())
                .build()));
    }

    @Test
    public void testTopoEntityToTypeSpecific() {
        final TypeSpecificPartialEntity typeSpecificPartialEntity =
            converter.createPartialEntity(ENTITY, Type.TYPE_SPECIFIC).getTypeSpecific();

        assertThat(typeSpecificPartialEntity.getTypeSpecificInfo(), is(ENTITY.getTypeSpecificInfo()));
        assertThat(typeSpecificPartialEntity.getOid(), is(ENTITY.getOid()));
        assertThat(typeSpecificPartialEntity.getDisplayName(), is(ENTITY.getDisplayName()));
    }

    @Test
    public void testTopoEntityToWithConnections() {
        final EntityWithConnections withConnections =
            converter.createPartialEntity(ENTITY, Type.WITH_CONNECTIONS).getWithConnections();

        assertThat(withConnections.getOid(), is(ENTITY.getOid()));
        assertThat(withConnections.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(withConnections.getConnectedEntitiesList(), is(ENTITY.getConnectedEntityListList()));
    }

    @Test
    public void testTopoEntityToFull() {
        final TopologyEntityDTO fullEntity = converter.createPartialEntity(ENTITY, Type.FULL).getFullEntity();
        assertThat(fullEntity, is(ENTITY));
    }

    @Test
    public void testGraphEntityToMinimal() {
        final MinimalEntity minEntity = converter.createPartialEntity(graphEntity, Type.MINIMAL).getMinimal();
        assertThat(minEntity.getOid(), is(graphEntity.getOid()));
        assertThat(minEntity.getEnvironmentType(), is(graphEntity.getEnvironmentType()));
        assertThat(minEntity.getDisplayName(), is(graphEntity.getDisplayName()));
        assertThat(minEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(minEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
    }

    @Test
    public void testGraphEntityToAction() {
        final ActionPartialEntity actionEntity = converter.createPartialEntity(graphEntity, Type.ACTION).getAction();
        assertThat(actionEntity.getOid(), is(graphEntity.getOid()));
        assertThat(actionEntity.getDisplayName(), is(graphEntity.getDisplayName()));
        assertThat(actionEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(actionEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(actionEntity.getCommoditySoldList(), contains(VMEM_SOLD));
    }

    @Test
    public void testGraphEntityToApi() {
        final ApiPartialEntity apiEntity = converter.createPartialEntity(graphEntity, Type.API).getApi();
        assertThat(apiEntity.getOid(), is(ENTITY.getOid()));
        assertThat(apiEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(apiEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(apiEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(apiEntity.getEntityState(), is(ENTITY.getEntityState()));
        assertThat(apiEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
        assertThat(apiEntity.getTags(), is(ENTITY.getTags()));
        assertThat(apiEntity.getProvidersList(), contains(RelatedEntity.newBuilder()
            .setEntityType(PROVIDER.getEntityType())
            .setOid(PROVIDER.getOid())
            .build()));
        assertThat(apiEntity.getConnectedToList(), contains(
            RelatedEntity.newBuilder()
                .setEntityType(CONNECTED_TO.getEntityType())
                .setOid(CONNECTED_TO.getOid())
                .build(),
            RelatedEntity.newBuilder()
                .setEntityType(OWNS.getEntityType())
                .setOid(OWNS.getOid())
                .build()));
    }

    @Test
    public void testGraphEntityToTypeSpecific() {
        final TypeSpecificPartialEntity typeSpecificPartialEntity =
            converter.createPartialEntity(graphEntity, Type.TYPE_SPECIFIC).getTypeSpecific();

        assertThat(typeSpecificPartialEntity.getTypeSpecificInfo(), is(ENTITY.getTypeSpecificInfo()));
        assertThat(typeSpecificPartialEntity.getOid(), is(ENTITY.getOid()));
        assertThat(typeSpecificPartialEntity.getDisplayName(), is(ENTITY.getDisplayName()));
    }

    @Test
    public void testGraphEntityToWithConnections() {
        final EntityWithConnections withConnections =
            converter.createPartialEntity(graphEntity, Type.WITH_CONNECTIONS).getWithConnections();

        assertThat(withConnections.getOid(), is(ENTITY.getOid()));
        assertThat(withConnections.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(withConnections.getConnectedEntitiesList(), is(ENTITY.getConnectedEntityListList()));
    }

    @Test
    public void testGraphEntityToFull() {
        final TopologyEntityDTO fullEntity = converter.createPartialEntity(graphEntity, Type.FULL).getFullEntity();
        assertThat(fullEntity, is(ENTITY));
    }

}
