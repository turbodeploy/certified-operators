package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
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
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;

public class PartialEntityConverterTest {

    private static final long TARGET_ID = 123L;

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private PartialEntityConverter converter = new PartialEntityConverter(liveTopologyStore);

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

    private RepoGraphEntity graphEntity;

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
            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                .putDiscoveredTargetData(TARGET_ID, PerTargetEntityInformation.getDefaultInstance())))
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setPhysicalMachine(PhysicalMachineInfo.newBuilder().setNumCpus(2)))
        .build();

    private RepoGraphEntity hostForVSANEntity;
    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @Before
    public void setup() {
        RepoGraphEntity.Builder providerBldr = RepoGraphEntity.newBuilder(PROVIDER);
        RepoGraphEntity.Builder connectedToBldr = RepoGraphEntity.newBuilder(CONNECTED_TO);
        RepoGraphEntity.Builder ownsBldr = RepoGraphEntity.newBuilder(OWNS);
        DefaultTagIndex tagIndex = DefaultTagIndex.singleEntity(ENTITY.getOid(), ENTITY.getTags());
        RepoGraphEntity.Builder graphBldr = RepoGraphEntity.newBuilder(ENTITY, tagIndex, new SharedByteBuffer());
        graphBldr.addProvider(providerBldr);
        graphBldr.addConsumer(RepoGraphEntity.newBuilder(CONSUMER));
        graphBldr.addOutboundAssociation(connectedToBldr);
        graphBldr.addOwnedEntity(ownsBldr);

        graphEntity = graphBldr.build();
        when(userSessionContext.isUserScoped()).thenReturn(false);

        RepoGraphEntity.Builder hostForVSANBldr = RepoGraphEntity.newBuilder(HOST_FOR_VSAN);
        hostForVSANBldr.addConsumer(RepoGraphEntity.newBuilder(VSAN));
        hostForVSANEntity = hostForVSANBldr.build();
    }

    @Test
    public void testTopoEntityToMinimal() {
        final MinimalEntity minEntity = converter.createPartialEntity(ENTITY, Type.MINIMAL, userSessionContext).getMinimal();
        assertThat(minEntity.getOid(), is(ENTITY.getOid()));
        assertThat(minEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(minEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(minEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(minEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
        assertThat(minEntity.getEntityState(), is(EntityState.POWERED_OFF));
    }

    @Test
    public void testTopoEntityToAction() {
        final ActionPartialEntity actionEntity = converter.createPartialEntity(ENTITY, Type.ACTION,
                userSessionContext).getAction();
        assertThat(actionEntity.getOid(), is(ENTITY.getOid()));
        assertThat(actionEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(actionEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(actionEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(actionEntity.getCommTypesWithHotReplaceList(), contains(UICommodityType.VMEM.typeNumber()));
    }

    @Test
    public void testTopoEntityToApi() {
        final ApiPartialEntity apiEntity = converter.createPartialEntity(ENTITY, Type.API, userSessionContext).getApi();
        assertThat(apiEntity.getOid(), is(ENTITY.getOid()));
        assertThat(apiEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(apiEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(apiEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(apiEntity.getEntityState(), is(ENTITY.getEntityState()));
        assertThat(apiEntity.getDiscoveredTargetDataMap().keySet(), contains(TARGET_ID));
        assertThat(apiEntity.getTags(), is(ENTITY.getTags()));
        Assert.assertThat(apiEntity.getStale(), Matchers.is(ENTITY.getStale()));
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
    public void testTopoEntityToWithConnections() {
        final EntityWithConnections withConnections =
            converter.createPartialEntity(ENTITY, Type.WITH_CONNECTIONS, userSessionContext).getWithConnections();

        assertThat(withConnections.getOid(), is(ENTITY.getOid()));
        assertThat(withConnections.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(withConnections.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(withConnections.getConnectedEntitiesList(), is(ENTITY.getConnectedEntityListList()));
    }

    @Test
    public void testTopoEntityToFull() {
        final TopologyEntityDTO fullEntity = converter.createPartialEntity(ENTITY, Type.FULL, userSessionContext).getFullEntity();
        assertThat(fullEntity, is(ENTITY));
    }

    @Test
    public void testGraphEntityToMinimal() {
        final MinimalEntity minEntity = converter.createPartialEntities(Stream.of(graphEntity), Type.MINIMAL, userSessionContext).findFirst().get().getMinimal();
        assertThat(minEntity.getOid(), is(graphEntity.getOid()));
        assertThat(minEntity.getEnvironmentType(), is(graphEntity.getEnvironmentType()));
        assertThat(minEntity.getDisplayName(), is(graphEntity.getDisplayName()));
        assertThat(minEntity.getEntityType(), is(graphEntity.getEntityType()));
        assertThat(minEntity.getDiscoveringTargetIdsList(), contains(TARGET_ID));
    }

    @Test
    public void testGraphEntityToApi() {
        final DefaultTagIndex tagIndex = mock(DefaultTagIndex.class);
        final UserSessionContext userSessionContext = mock(UserSessionContext.class);
        final Long2ObjectMap<Map<String, Set<String>>> map = new Long2ObjectOpenHashMap<>();
        map.put(graphEntity.getOid(), Collections.singletonMap("tag", Collections.singleton("val")));
        when(tagIndex.getTagsByEntity(any())).thenReturn(map);
        final SourceRealtimeTopology mockTopology = mock(SourceRealtimeTopology.class);
        when(mockTopology.globalTags()).thenReturn(tagIndex);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(mockTopology));

        final ApiPartialEntity apiEntity = converter.createPartialEntities(Stream.of(graphEntity), Type.API, userSessionContext).findFirst().get().getApi();
        assertThat(apiEntity.getOid(), is(ENTITY.getOid()));
        assertThat(apiEntity.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(apiEntity.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(apiEntity.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(apiEntity.getEntityState(), is(ENTITY.getEntityState()));
        assertThat(apiEntity.getDiscoveredTargetDataMap().keySet(), contains(TARGET_ID));
        assertThat(apiEntity.getTags(), is(ENTITY.getTags()));
        Assert.assertThat(apiEntity.getStale(), Matchers.is(ENTITY.getStale()));
        assertThat(apiEntity.getProvidersList(), contains(RelatedEntity.newBuilder()
            .setEntityType(PROVIDER.getEntityType())
            .setOid(PROVIDER.getOid())
            .setDisplayName(PROVIDER.getDisplayName())
            .build()));
        assertThat(apiEntity.getConsumersList(), contains(RelatedEntity.newBuilder()
            .setEntityType(CONSUMER.getEntityType())
            .setOid(CONSUMER.getOid())
            .setDisplayName(CONSUMER.getDisplayName())
            .build()));
        assertThat(apiEntity.getConnectedToList(), contains(
            RelatedEntity.newBuilder()
                .setEntityType(CONNECTED_TO.getEntityType())
                .setOid(CONNECTED_TO.getOid())
                .setDisplayName(CONNECTED_TO.getDisplayName())
                .build(),
            RelatedEntity.newBuilder()
                .setEntityType(OWNS.getEntityType())
                .setOid(OWNS.getOid())
                .setDisplayName(OWNS.getDisplayName())
                .build()));
    }

    /**
     * Test converting a graph entity for a scoped user. Make sure that entities not in the scope
     * are filtered out in the providers and consumers fields.
     */
    @Test
    public void testConversionWithScope() {
        final DefaultTagIndex tagIndex = mock(DefaultTagIndex.class);
        final EntityAccessScope accessScope = mock(EntityAccessScope.class);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        when(accessScope.contains(ENTITY.getOid())).thenReturn(true);
        final Long2ObjectMap<Map<String, Set<String>>> map = new Long2ObjectOpenHashMap<>();
        map.put(graphEntity.getOid(), Collections.singletonMap("tag", Collections.singleton("val")));
        when(tagIndex.getTagsByEntity(any())).thenReturn(map);
        final SourceRealtimeTopology mockTopology = mock(SourceRealtimeTopology.class);
        when(mockTopology.globalTags()).thenReturn(tagIndex);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(mockTopology));

        final ApiPartialEntity apiEntity = converter.createPartialEntities(Stream.of(graphEntity), Type.API, userSessionContext).findFirst().get().getApi();
        Assert.assertEquals(apiEntity.getStale(), graphEntity.isStale());
        assertEquals(apiEntity.getProvidersCount(), 0);
        assertEquals(apiEntity.getConsumersCount(), 0);
        assertEquals(apiEntity.getConnectedToCount(), 0);

        final EntityWithConnections withConnections =
                converter.createPartialEntities(Stream.of(graphEntity), Type.WITH_CONNECTIONS, userSessionContext).findFirst().get().getWithConnections();
        assertEquals(withConnections.getConnectedEntitiesCount(), 0);

        final TopologyEntityDTO fullEntity = converter.createPartialEntities(Stream.of(graphEntity), Type.FULL, userSessionContext).findFirst().get().getFullEntity();
        assertEquals(fullEntity.getConnectedEntityListCount(), 0);

        final ActionPartialEntity actionEntity = converter.createPartialEntity(ENTITY, Type.ACTION,
                userSessionContext).getAction();
        assertEquals(actionEntity.getConnectedEntitiesCount(), 0);
    }

    @Test
    public void testGraphEntityToWithConnections() {
        final EntityWithConnections withConnections =
            converter.createPartialEntities(Stream.of(graphEntity), Type.WITH_CONNECTIONS, userSessionContext).findFirst().get().getWithConnections();

        assertThat(withConnections.getOid(), is(ENTITY.getOid()));
        assertThat(withConnections.getDisplayName(), is(ENTITY.getDisplayName()));
        assertThat(withConnections.getEntityType(), is(ENTITY.getEntityType()));
        assertThat(withConnections.getConnectedEntitiesList(), is(ENTITY.getConnectedEntityListList()));
    }

    /**
     * Tests that {@link PartialEntityConverter} correctly creates
     * EntityWithOnlyEnvironmentTypeAndTargets objects.
     */
    @Test
    public void testGraphEntityToWithOnlyEnvironmentTypeAndTargets() {
        final EntityWithOnlyEnvironmentTypeAndTargets withOnlyEnvTypeAndTargets =
                converter.createPartialEntities(Stream.of(graphEntity),
                                Type.WITH_ONLY_ENVIRONMENT_TYPE_AND_TARGETS, userSessionContext)
                        .findFirst().get().getWithOnlyEnvironmentTypeAndTargets();

        assertThat(withOnlyEnvTypeAndTargets.getOid(), is(ENTITY.getOid()));
        assertThat(withOnlyEnvTypeAndTargets.getEnvironmentType(), is(ENTITY.getEnvironmentType()));
        assertThat(withOnlyEnvTypeAndTargets.getDiscoveringTargetIdsList(), contains(TARGET_ID));
    }

    @Test
    public void testGraphEntityToFull() {
        final TopologyEntityDTO fullEntity = converter.createPartialEntities(Stream.of(graphEntity), Type.FULL, userSessionContext).findFirst().get().getFullEntity();
        assertThat(fullEntity, is(ENTITY));
    }
}
