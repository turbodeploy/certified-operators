package com.vmturbo.topology.graph.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.SharedByteBuffer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TestGraphEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseSearchableTopology.BaseSearchableTopologyBuilder;

/**
 * Tests for the {@link BaseTopologyStore} and {@link BaseTopology}.
 */
public class BaseTopologyStoreTest {

    TestTopologyStore testTopologyStore = new TestTopologyStore();

    private static final TopologyInfo TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setTopologyContextId(777)
            .build();

    /**
     * Test adding some entities into a topology, and verifying that the connections get built
     * up properly.
     */
    @Test
    public void testBuilder() {
        TestSearchableTopologyBuilder bldr = testTopologyStore.newRealtimeSourceTopology(TOPOLOGY_INFO);
        bldr.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(7L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setDisplayName("myVm")
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(8L)
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setConnectedEntityId(9L))
            .build());
        bldr.addEntity(TopologyEntityDTO.newBuilder()
            .setOid(8L)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setDisplayName("pm")
        .build());
        bldr.addEntity(TopologyEntityDTO.newBuilder()
                .setOid(9L)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setDisplayName("volume")
                .build());
        TestSearchableTopology topology = bldr.finish();
        TestGraphEntity vm = topology.entityGraph().getEntity(7L).get();
        assertThat(vm.getProviders().stream().map(e -> e.getOid()).findFirst().get(), is(8L));
        assertThat(vm.getOutboundAssociatedEntities().stream().map(e -> e.getOid()).findFirst().get(), is(9L));
    }

    /**
     * Tests updating a entity state in the realtime topology.
     */
    @Test
    public void testUpdateEntityWithNewState() {
        TopologyInfo tInfo = TopologyInfo.newBuilder()
                .setTopologyId(7)
                .setCreationTime(0)
                .build();
        final TestSearchableTopologyBuilder bldr = testTopologyStore.newRealtimeSourceTopology(tInfo);
        final TopologyEntityDTO originalHost = TopologyEntityDTO.newBuilder()
                .setOid(7L)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setDisplayName("foo")
                .build();
        bldr.addEntity(originalHost);
        bldr.finish();

        TopologyEntityDTO updatedHost = originalHost.toBuilder()
                .setEntityState(EntityState.MAINTENANCE).build();
        testTopologyStore.updateEntityWithNewState(updatedHost);
        final TestSearchableTopology topo = testTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.MAINTENANCE,
                topo.entityGraph().getEntity(originalHost.getOid()).get().getEntityState());
    }

    /**
     * Tests that entity states cached in entitiesWithUpdatedState get applied correctly to
     * incoming topologies, and don't get applied to topologies that are more recent than the
     * state change.
     */
    @Test
    public void testUpdateEntityWithNewStateCache() {
        final TopologyEntityDTO originalHost = TopologyEntityDTO.newBuilder()
                .setOid(7L)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_OFF)
                .setDisplayName("foo")
                .build();
        final TopologyEntityDTO host1PoweredOn = originalHost.toBuilder()
                .setEntityState(EntityState.POWERED_ON)
                .build();

        final TopologyEntityDTO host1InMaintenance = originalHost.toBuilder()
                .setEntityState(EntityState.MAINTENANCE)
                .build();

        TopologyInfo tInfo = TopologyInfo.newBuilder()
                .setTopologyId(0)
                .setCreationTime(0)
                .build();

        EntitiesWithNewState poweredOnStateChange = EntitiesWithNewState
                .newBuilder().addTopologyEntity(host1PoweredOn)
                .setStateChangeId(1)
                .build();

        EntitiesWithNewState maintenanceStateChange = EntitiesWithNewState
                .newBuilder().addTopologyEntity(host1InMaintenance)
                .setStateChangeId(2)
                .build();

        testTopologyStore.setEntityWithUpdatedState(poweredOnStateChange);
        testTopologyStore.setEntityWithUpdatedState(maintenanceStateChange);

        final TestSearchableTopologyBuilder bldr = testTopologyStore.newRealtimeSourceTopology(tInfo);
        bldr.addEntity(originalHost);
        bldr.finish();
        final TestSearchableTopology topo = testTopologyStore.getSourceTopology().get();

        // Check the topology is updated with the host state in the entityWithNewState cache
        Assert.assertEquals(EntityState.MAINTENANCE,
                topo.entityGraph().getEntity(originalHost.getOid()).get().getEntityState());

        // New topology, with a more updated time stamp then the host state change, should not
        // get affected by the entityWithNewStateCache
        final TestSearchableTopologyBuilder updatedBldr = testTopologyStore
                .newRealtimeSourceTopology(tInfo.toBuilder().setTopologyId(3).build());
        updatedBldr.addEntity(originalHost);
        updatedBldr.finish();
        final TestSearchableTopology updatedTopo = testTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.POWERED_ON,
                updatedTopo.entityGraph().getEntity(originalHost.getOid()).get().getEntityState());
    }

    /**
     * Implementation of {@link BaseSearchableTopology}.
     */
    public static class TestSearchableTopology extends BaseSearchableTopology<TestGraphEntity> {

        protected TestSearchableTopology(TopologyInfo topologyInfo,
                TopologyGraph<TestGraphEntity> entityGraph) {
            super(topologyInfo, entityGraph);
        }
    }

    /**
     * Implementation of {@link BaseSearchableTopologyBuilder}.
     */
    public static class TestSearchableTopologyBuilder extends BaseSearchableTopologyBuilder<TestSearchableTopology, TestGraphEntity, TestGraphEntity.Builder> {

        protected TestSearchableTopologyBuilder(DataMetricSummary constructionTimeSummary,
                @Nonnull TopologyInfo topologyInfo,
                @Nonnull Consumer<TestSearchableTopology> onFinish) {
            super(constructionTimeSummary, topologyInfo, onFinish);
        }

        @Override
        protected TestSearchableTopology newTopology(TopologyInfo topologyInfo,
                TopologyGraph<TestGraphEntity> graph) {
            return new TestSearchableTopology(topologyInfo, graph);
        }

        @Override
        protected Builder newBuilder(TopologyEntityDTO entity, DefaultTagIndex tagIndex,
                SharedByteBuffer compressionBuffer) {
            return new TestGraphEntity.Builder(entity);
        }
    }

    /**
     * Implementation of {@link TestTopologyStore}.
     */
    public static class TestTopologyStore extends BaseTopologyStore<TestSearchableTopology, TestSearchableTopologyBuilder, TestGraphEntity, Builder> {

        @Override
        protected TestSearchableTopologyBuilder newBuilder(TopologyInfo topologyInfo,
                Consumer<TestSearchableTopology> consumer) {
            return new TestSearchableTopologyBuilder(mock(DataMetricSummary.class), topologyInfo, consumer);
        }
    }

}