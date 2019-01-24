package com.vmturbo.repository.listener;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.topology.IncrementalTopologyRelationshipRecorder;

/**
 * Unit tests for {@link IncrementalTopologyRelationshipRecorder}.
 *
 */
public class IncrementalTopologyRelationshipRecorderTest {

    private static final long vmId = 1L;
    private static final long volumeId = 2L;
    private static final long businessAccountId = 3L;
    private static final long storageTierId = 4L;
    private static final long computeTierId = 5L;
    private static final long cloudServiceId = 6L;
    private static final long zoneId = 7L;
    private static final long regionId = 8L;

    private final TopologyEntityDTO datacenter = TopologyEntityDTO.newBuilder()
        .setOid(71930913569376L)
        .setEntityType(EntityType.DATACENTER.getNumber())
        .build();

    private final TopologyEntityDTO physicalMachine = TopologyEntityDTO.newBuilder()
        .setOid(1234567890L)
        .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder().setProviderId(71930913569376L).build())
        .build();

    private final TopologyEntityDTO virtualMachine = TopologyEntityDTO.newBuilder()
            .setOid(vmId)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(zoneId)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(volumeId)
                    .build())
            .build();

    private final TopologyEntityDTO volume = TopologyEntityDTO.newBuilder()
            .setOid(volumeId)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(zoneId)
                    .build())
            .build();

    private final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
            .setOid(businessAccountId)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(vmId)
                    .build())
            .build();

    private final TopologyEntityDTO storageTier = TopologyEntityDTO.newBuilder()
            .setOid(storageTierId)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(regionId)
                    .build())
            .build();

    private final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
            .setOid(computeTierId)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(regionId)
                    .build())
            .build();

    private final TopologyEntityDTO cloudService = TopologyEntityDTO.newBuilder()
            .setOid(cloudServiceId)
            .setEntityType(EntityType.CLOUD_SERVICE_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(computeTierId)
                    .build())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                    .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                    .setConnectedEntityId(storageTierId)
                    .build())
            .build();

    private final TopologyEntityDTO zone = TopologyEntityDTO.newBuilder()
            .setOid(zoneId)
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .build();

    private final TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
            .setOid(regionId)
            .setEntityType(EntityType.REGION_VALUE)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION)
                    .setConnectedEntityId(zoneId)
                    .build())
            .build();

    private final List<TopologyEntityDTO> pmChunk = Collections.singletonList(physicalMachine);
    private final List<TopologyEntityDTO> datacenterChunk = Collections.singletonList(datacenter);

    @Test
    public void testDependenciesArriveInSeparateChunksInForwardOrder() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        recorder.processChunk(datacenterChunk);
        recorder.processChunk(pmChunk);

        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(
            RepoEntityType.DATACENTER.getValue(),
            providerRelationships.get(RepoEntityType.PHYSICAL_MACHINE.getValue()).iterator().next()
        );
    }

    @Test
    public void testDependenciesArriveInSeparateChunksInReverseOrder() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        recorder.processChunk(pmChunk);
        recorder.processChunk(datacenterChunk);

        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(
            RepoEntityType.DATACENTER.getValue(),
            providerRelationships.get(RepoEntityType.PHYSICAL_MACHINE.getValue()).iterator().next()
        );
    }

    /**
     * Test that the supply chain produced for the whole collection of DTOs in one chunk
     * is the same as the one produced when the collection is chunked.
     * @throws InterruptedException if generating the test topology is interrupted
     */
    @Test
    public void testSupplyChainChunks() throws InterruptedException {
        List<TopologyEntityDTO> topology = TopologyUtils.generateTopology(200);
        IncrementalTopologyRelationshipRecorder recorder0 = new IncrementalTopologyRelationshipRecorder();
        recorder0.processChunk(topology);
        Multimap<String, String> noChunksSupplyChain = recorder0.supplyChain();
        for (int i = 5; i < 50; i *= 1.5) {
            final List<List<TopologyEntityDTO>> chunks =
                            Lists.partition(ImmutableList.copyOf(topology), i);
            IncrementalTopologyRelationshipRecorder recorder =
                            new IncrementalTopologyRelationshipRecorder();
            chunks.stream().forEach(recorder::processChunk);
            assertEquals("Chunk size : " + i, noChunksSupplyChain, recorder.supplyChain());
        }
    }

    @Test
    public void testProcessCloudEntitiesInOneChunk() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        List<TopologyEntityDTO> cloudEntitiesChunk = Lists.newArrayList(
                businessAccount, virtualMachine, volume, cloudService, storageTier, computeTier, zone, region
        );
        recorder.processChunk(cloudEntitiesChunk);
        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(3, providerRelationships.keySet().size());
        assertThat(providerRelationships.get(RepoEntityType.VIRTUAL_MACHINE.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue(),
                        RepoEntityType.VIRTUAL_VOLUME.getValue()));
        assertThat(providerRelationships.get(RepoEntityType.VIRTUAL_VOLUME.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue()));
        assertThat(providerRelationships.get(RepoEntityType.REGION.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue()));
    }

    @Test
    public void testProcessCloudEntitiesChunksInReverseOrder() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        List<TopologyEntityDTO> cloudEntitiesChunk1 = Lists.newArrayList(
                businessAccount, virtualMachine, cloudService, volume, region
        );
        List<TopologyEntityDTO> cloudEntitiesChunk2 = Lists.newArrayList(
                storageTier, computeTier, zone
        );
        recorder.processChunk(cloudEntitiesChunk1);
        recorder.processChunk(cloudEntitiesChunk2);
        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(3, providerRelationships.keySet().size());
        assertThat(providerRelationships.get(RepoEntityType.VIRTUAL_MACHINE.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue(),
                        RepoEntityType.VIRTUAL_VOLUME.getValue()));
        assertThat(providerRelationships.get(RepoEntityType.VIRTUAL_VOLUME.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue()));
        assertThat(providerRelationships.get(RepoEntityType.REGION.getValue()),
                containsInAnyOrder(RepoEntityType.AVAILABILITY_ZONE.getValue()));
    }
}
