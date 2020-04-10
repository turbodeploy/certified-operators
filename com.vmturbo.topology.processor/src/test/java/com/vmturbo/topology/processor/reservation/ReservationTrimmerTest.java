package com.vmturbo.topology.processor.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.reservation.ReservationTrimmer.TrimmingSummary;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

/**
 * Unit tests for {@link ReservationTrimmer}.
 */
public class ReservationTrimmerTest {

    private final int relevantCommodity = 10;

    private final TopologyEntityDTO.Builder reservationBuilder =
        TopologyEntityDTO.newBuilder()
            .setOid(1)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setDisplayName("my reservation")
            .setOrigin(Origin.newBuilder()
                .setReservationOrigin(ReservationOrigin.newBuilder()
                    .setReservationId(123)))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(relevantCommodity))));

    /**
     * Test that irrelevant entities commodities get removed.
     */
    @Test
    public void testRemoveIrrelevantEntities() {
        final TopologyGraph<TopologyEntity> inputGraph = TopologyEntityUtils.topologyGraphOf(
            TopologyEntityUtils.topologyEntityBuilder(reservationBuilder),
            TopologyEntityUtils.topologyEntity(2, EntityType.PHYSICAL_MACHINE),
            TopologyEntityUtils.topologyEntity(3, EntityType.DESKTOP_POOL));

        final TrimmingSummary summary = new ReservationTrimmer().trimTopologyGraph(inputGraph);
        final TopologyGraph<TopologyEntity> newGraph = summary.getNewGraph();
        // Should have removed one entity.
        assertThat(newGraph.size(), is(2));
        assertThat(newGraph.entities().map(TopologyEntity::getOid).collect(Collectors.toSet()),
            containsInAnyOrder(reservationBuilder.getOid(), 2L));

        assertThat(summary.toString(), containsString("Removed 1 providers"));
    }

    /**
     * Test that irrelevant bought/sold commodities get removed.
     */
    @Test
    public void testRemoveIrrelevantCommodities() {
        // PM buying commodity 1 from storage.
        // Storage buying commodity 7 from some other provider.
        // PM and storage selling unnecessary commodities.
        // PM and storage selling relevant commodity.
        // PM and storage selling DPSM Access commodities.
        final long storageId = 2;
        final long hostId = 3;
        TopologyEntity.Builder pm = TopologyEntityUtils.topologyEntityBuilder(TopologyEntityDTO.newBuilder()
            .setOid(hostId)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setDisplayName("my host")
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(relevantCommodity)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    // Some irrelevant commodity
                    .setType(123)))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(storageId)
                .setProviderEntityType(EntityType.STORAGE_VALUE)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        // Buying from storage.
                        .setType(1)))));
        TopologyEntity.Builder storage = TopologyEntityUtils.topologyEntityBuilder(TopologyEntityDTO.newBuilder()
            .setOid(storageId)
            .setEntityType(EntityType.STORAGE_VALUE)
            .setDisplayName("my storage")
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(relevantCommodity)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    // Some irrelevant commodity
                    .setType(478)))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                // Some irrelevant provider
                .setProviderId(999)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        // Buying from some other provider.
                        .setType(7)))));

        final TopologyGraph<TopologyEntity> inputGraph = TopologyEntityUtils.topologyGraphOf(
            TopologyEntityUtils.topologyEntityBuilder(reservationBuilder),
            storage, pm);

        final TrimmingSummary summary = new ReservationTrimmer().trimTopologyGraph(inputGraph);
        final TopologyGraph<TopologyEntity> newGraph = summary.getNewGraph();
        // Should not have removed entities.
        assertThat(newGraph.size(), is(3));

        final TopologyEntityDTO.Builder storageBldr = newGraph.getEntity(storageId).get().getTopologyEntityDtoBuilder();
        assertThat(storageBldr.getCommoditySoldListList().stream()
            .map(commSold -> commSold.getCommodityType().getType())
            .collect(Collectors.toList()), containsInAnyOrder(relevantCommodity, CommodityDTO.CommodityType.DSPM_ACCESS_VALUE));
        assertTrue(storageBldr.getCommoditiesBoughtFromProvidersList().isEmpty());

        final TopologyEntityDTO.Builder pmBldr = newGraph.getEntity(hostId).get().getTopologyEntityDtoBuilder();
        assertThat(pmBldr.getCommoditySoldListList().stream()
            .map(commSold -> commSold.getCommodityType().getType())
            .collect(Collectors.toList()), containsInAnyOrder(relevantCommodity, CommodityDTO.CommodityType.DSPM_ACCESS_VALUE));
        // Should be unchanged - we keep the only commodity that was there before.
        assertThat(pmBldr.getCommoditiesBoughtFromProvidersList(),
            is(pm.getEntityBuilder().getCommoditiesBoughtFromProvidersList()));

        assertThat(newGraph.entities().map(TopologyEntity::getOid).collect(Collectors.toSet()),
            containsInAnyOrder(reservationBuilder.getOid(), storageId, hostId));
        assertThat(summary.toString(), containsString("Removed 3 commodities"));
    }

}