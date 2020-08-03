package com.vmturbo.repository.listener.realtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.DiscoveryOriginBuilder;
import com.vmturbo.stitching.StitchingMergeInformation;

/**
 * Unit tests for {@link RepoGraphEntity}.
 */
public class RepoGraphEntityTest {
    /**
     * Auxiliary entity builder.
     */
    private static final long ENTITY_OID = 23345;
    private static final int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;
    private final TopologyEntityDTO.Builder dtoBuilder =
        TopologyEntityDTO.newBuilder().setOid(ENTITY_OID).setEntityType(ENTITY_TYPE);
    /**
     * Builders for a simple cloud topology: region owns zone aggregates VM.
     */
    private RepoGraphEntity.Builder vmBuilder;
    private RepoGraphEntity.Builder azBuilder;
    private RepoGraphEntity.Builder rgBuilder;

    /**
     * Tests building of a single entity.
     */
    @Test
    public void testBuild() {
        final RepoGraphEntity entity = RepoGraphEntity.newBuilder(dtoBuilder.build()).build();

        assertEquals(ENTITY_OID, entity.getOid());
        assertEquals(ENTITY_TYPE, entity.getEntityType());
        assertFalse(entity.getDiscoveringTargetIds().findAny().isPresent());
    }

    /**
     * Tests building with a discovery target.
     */
    @Test
    public void testBuildDiscoveryInformation() {
        final long discoveryTargetId = 111L;
        final Origin origin = Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOriginBuilder
                                                        .discoveredBy(discoveryTargetId)
                                                        .lastUpdatedAt(0L))
                                .build();
        final RepoGraphEntity entity = RepoGraphEntity.newBuilder(dtoBuilder.setOrigin(origin).build())
                                            .build();
        final List<Long> originIds = entity.getDiscoveringTargetIds().collect(Collectors.toList());

        assertEquals(1, originIds.size());
        assertEquals(discoveryTargetId, (long)originIds.get(0));
    }

    /**
     * Tests building with multiple targets.
     */
    @Test
    public void testBuildDiscoveryInformationWithMergeFromTargetIds() {
        final long discoveryTargetId = 111L;
        final StitchingMergeInformation mergedTarget1 =
                    new StitchingMergeInformation(1L, 333L,
                                                  StitchingErrors.none());
        final StitchingMergeInformation mergedTarget2 =
                    new StitchingMergeInformation(1L, 444L,
                                                  StitchingErrors.none());
        final Origin origin =
            Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOriginBuilder
                                        .discoveredBy(discoveryTargetId)
                                        .withMerge(mergedTarget1, mergedTarget2)
                                        .lastUpdatedAt(0L))
                .build();
        final RepoGraphEntity entity = RepoGraphEntity.newBuilder(dtoBuilder.setOrigin(origin).build())
                                            .build();
        final List<Long> originIds = entity.getDiscoveringTargetIds().collect(Collectors.toList());

        assertEquals(3, originIds.size());
        assertThat(originIds, containsInAnyOrder(discoveryTargetId, mergedTarget1.getTargetId(),
                                                 mergedTarget2.getTargetId()));
    }

    /**
     * Tests clearing of consumers and providers.
     */
    @Test
    public void testClearConsumersAndProviders() {
        final RepoGraphEntity.Builder consumer = RepoGraphEntity.newBuilder(dtoBuilder.build());
        final RepoGraphEntity.Builder provider = RepoGraphEntity.newBuilder(dtoBuilder.build());

        consumer.addProvider(provider);
        provider.addConsumer(consumer);
        assertEquals(1, consumer.build().getProviders().size());
        assertEquals(provider.build(), consumer.build().getProviders().get(0));
        assertEquals(1, provider.build().getConsumers().size());
        assertEquals(consumer.build(), provider.build().getConsumers().get(0));

        consumer.clearConsumersAndProviders();
        provider.clearConsumersAndProviders();

        assertEquals(0, consumer.build().getProviders().size());
        assertEquals(0, provider.build().getConsumers().size());
    }

    /**
     * Tests that the connection getters return the appropriate information in a small
     * topology where a region owns a zone which aggregates a VM.
     */
    @Test
    public void testConnections() {
        makeSimpleCloudTopology();

        final RepoGraphEntity vm = vmBuilder.build();
        final RepoGraphEntity az = azBuilder.build();
        final RepoGraphEntity rg = rgBuilder.build();

        assertFalse(vm.getOwner().isPresent());
        assertEquals(0, vm.getOwnedEntities().size());
        assertEquals(1, vm.getAggregators().size());
        assertEquals(vm.getAggregators().get(0), az);
        assertEquals(0, vm.getAggregatedEntities().size());
        assertEquals(0, vm.getAggregatedAndOwnedEntities().size());
        assertEquals(1, vm.getAggregatorsAndOwner().size());
        assertEquals(vm.getAggregatorsAndOwner().get(0), az);
        assertEquals(1, vm.getBroadcastConnections().size());
        assertEquals(1, vm.getBroadcastRelatedEntities().size());

        assertEquals(az.getOwner().get(), rg);
        assertEquals(0, az.getOwnedEntities().size());
        assertEquals(0, az.getAggregators().size());
        assertEquals(1, az.getAggregatedEntities().size());
        assertEquals(az.getAggregatedEntities().get(0), vm);
        assertEquals(1, az.getAggregatedAndOwnedEntities().size());
        assertEquals(az.getAggregatedAndOwnedEntities().get(0), vm);
        assertEquals(1, az.getAggregatorsAndOwner().size());
        assertEquals(az.getAggregatorsAndOwner().get(0), rg);
        assertEquals(0, az.getBroadcastConnections().size());
        assertEquals(0, az.getBroadcastRelatedEntities().size());

        assertFalse(rg.getOwner().isPresent());
        assertEquals(1, rg.getOwnedEntities().size());
        assertEquals(rg.getOwnedEntities().get(0), az);
        assertEquals(0, rg.getAggregators().size());
        assertEquals(0, rg.getAggregatedEntities().size());
        assertEquals(1, rg.getAggregatedAndOwnedEntities().size());
        assertEquals(rg.getAggregatedAndOwnedEntities().get(0), az);
        assertEquals(0, rg.getAggregatorsAndOwner().size());
        assertEquals(1, rg.getBroadcastConnections().size());
        assertEquals(1, rg.getBroadcastRelatedEntities().size());
    }

    /**
     * Tests that clearing connections work properly.
     */
    @Test
    public void testClearConnections() {
        makeSimpleCloudTopology();

        vmBuilder.clearConsumersAndProviders();
        azBuilder.clearConsumersAndProviders();
        rgBuilder.clearConsumersAndProviders();

        final RepoGraphEntity vm = vmBuilder.build();
        final RepoGraphEntity az = azBuilder.build();
        final RepoGraphEntity rg = rgBuilder.build();

        assertEntityHasNoConnections(vm);
        assertEntityHasNoConnections(az);
        assertEntityHasNoConnections(rg);
    }

    private void makeSimpleCloudTopology() {
        final long vmId = 1L;
        final long azId = 2L;
        final long rgId = 3L;

        vmBuilder = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                    .setOid(vmId)
                                                    .setDisplayName("foo")
                                                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                    .build());

        azBuilder = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                    .setOid(azId)
                                                    .setDisplayName("foo")
                                                    .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                                                    .build());

        rgBuilder = RepoGraphEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                    .setOid(rgId)
                                                    .setDisplayName("foo")
                                                    .setEntityType(EntityType.REGION_VALUE)
                                                    .build());

        vmBuilder.addAggregator(azBuilder);
        azBuilder.addAggregatedEntity(vmBuilder);
        azBuilder.addOwner(rgBuilder);
        rgBuilder.addOwnedEntity(azBuilder);
    }

    private void assertEntityHasNoConnections(RepoGraphEntity entity) {
        assertFalse(entity.getOwner().isPresent());
        assertEquals(0, entity.getOwnedEntities().size());
        assertEquals(0, entity.getAggregators().size());
        assertEquals(0, entity.getAggregatedEntities().size());
        assertEquals(0, entity.getAggregatedAndOwnedEntities().size());
        assertEquals(0, entity.getAggregatorsAndOwner().size());
    }
}
