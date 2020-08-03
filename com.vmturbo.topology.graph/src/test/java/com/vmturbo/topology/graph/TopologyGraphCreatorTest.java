package com.vmturbo.topology.graph;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Tests {@link TopologyGraphCreator}.
 */
public class TopologyGraphCreatorTest {
    /**
     * Tests the following scenario used to build a topology graph:
     * A bunch of entities are created and connected through their ids,
     * and then they are "fed" to the {@link TopologyGraphCreator} object
     * which creates the actual connections between them.
     */
    @Test
    public void testSimpleCloudTopology() {
        final long vmId = 1L;
        final long azId = 2L;
        final long rgId = 3L;
        final long dbId = 4L;

        final TestGraphEntity.Builder vmBuilder =
                TestGraphEntity.newBuilder(vmId, ApiEntityType.VIRTUAL_MACHINE);
        final TestGraphEntity.Builder azBuilder =
                TestGraphEntity.newBuilder(azId, ApiEntityType.AVAILABILITY_ZONE);
        final TestGraphEntity.Builder rgBuilder = TestGraphEntity.newBuilder(rgId, ApiEntityType.REGION);
        final TestGraphEntity.Builder dbBuilder = TestGraphEntity.newBuilder(dbId, ApiEntityType.DATABASE);

        rgBuilder.addConnectedEntity(azId, ConnectionType.OWNS_CONNECTION);
        vmBuilder.addConnectedEntity(azId, ConnectionType.AGGREGATED_BY_CONNECTION);
        dbBuilder.addProviderId(vmId).addConnectedEntity(azId, ConnectionType.AGGREGATED_BY_CONNECTION);

        final TopologyGraph<TestGraphEntity> topologyGraph =
                new TopologyGraphCreator<TestGraphEntity.Builder, TestGraphEntity>()
                    .addEntities(ImmutableSet.of(rgBuilder, azBuilder, vmBuilder, dbBuilder))
                    .build();
        final TestGraphEntity region = topologyGraph.getEntity(rgId).get();
        final TestGraphEntity zone = topologyGraph.getEntity(azId).get();
        final TestGraphEntity vm = topologyGraph.getEntity(vmId).get();
        final TestGraphEntity db = topologyGraph.getEntity(dbId).get();
        final List<TestGraphEntity> singletonZone = Collections.singletonList(zone);

        // owners of entities
        Assert.assertFalse(region.getOwner().isPresent());
        Assert.assertEquals(region, zone.getOwner().get());
        Assert.assertFalse(vm.getOwner().isPresent());
        Assert.assertFalse(db.getOwner().isPresent());

        // aggregators of entities
        Assert.assertTrue(region.getAggregators().isEmpty());
        Assert.assertTrue(zone.getAggregators().isEmpty());
        Assert.assertEquals(singletonZone, vm.getAggregators());
        Assert.assertEquals(singletonZone, db.getAggregators());

        // owned entities
        Assert.assertEquals(singletonZone, region.getOwnedEntities());
        Assert.assertTrue(zone.getOwnedEntities().isEmpty());
        Assert.assertTrue(vm.getOwnedEntities().isEmpty());
        Assert.assertTrue(db.getOwnedEntities().isEmpty());

        // aggregated entities
        Assert.assertTrue(region.getAggregatedEntities().isEmpty());
        Assert.assertEquals(ImmutableSet.of(vm, db),
                            zone.getAggregatedEntities().stream().collect(Collectors.toSet()));
        Assert.assertTrue(vm.getAggregatedEntities().isEmpty());
        Assert.assertTrue(db.getAggregatedEntities().isEmpty());

        // providers
        Assert.assertTrue(region.getProviders().isEmpty());
        Assert.assertTrue(zone.getProviders().isEmpty());
        Assert.assertTrue(vm.getProviders().isEmpty());
        Assert.assertEquals(Collections.singletonList(vm), db.getProviders());

        // consumers
        Assert.assertTrue(region.getConsumers().isEmpty());
        Assert.assertTrue(zone.getConsumers().isEmpty());
        Assert.assertEquals(Collections.singletonList(db), vm.getConsumers());
        Assert.assertTrue(db.getConsumers().isEmpty());
    }
}
