package com.vmturbo.topology.processor.ncm;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.NET_THROUGHPUT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.*;

public class FlowCommoditiesGenerator {
    /**
     * Generates Flow commodities.
     *
     * @param graph The graph with settings.
     */
    public void generateCommodities(final @Nonnull TopologyStitchingGraph graph) {
        // Get PMs.
        graph.entities().filter(e -> e.getEntityType().equals(PHYSICAL_MACHINE)).
                forEach(this::sellFlowCommoditiesPM);
    }

    /**
     * Builds sold Flow commodity.
     *
     * @param key      The key.
     * @param capacity The capacity.
     * @return The sold commodity builder.
     */
    private @Nonnull Builder buildSoldFlowComm(final @Nonnull String key,
                                               final double capacity) {
        return CommonDTO.CommodityDTO.newBuilder()
                .setCommodityType(CommonDTO.CommodityDTO.CommodityType.FLOW)
                .setActive(true)
                .setResizable(true)
                .setKey(key)
                .setCapacity(capacity);
    }

    /**
     * Build bought Flow commodity.
     *
     * @param providerID   The provider ID.
     * @param providerType The provider type..
     * @return The bought commodity.
     */
    private CommodityBought buildBoughtFlowComm(
            final String providerID,
            final CommonDTO.EntityDTO.EntityType providerType,
            final @Nonnull Builder soldComm) {
        return CommodityBought.newBuilder()
                .setProviderId(providerID)
                .setProviderType(providerType)
                .addBought(soldComm)
                .build();
    }

    /**
     * Sells the Flow commodities for a physical host.
     *
     * @param pm The physical host.
     */
    private void sellFlowCommoditiesPM(final @Nonnull TopologyStitchingEntity pm) {
        Optional<Builder> netThroughput = pm.getCommoditiesSold()
                .filter(c -> c.getCommodityType().equals(NET_THROUGHPUT)).findFirst();
        if (!netThroughput.isPresent()) {
            return;
        }
        // Set the capacities for the flows.
        double capacity = netThroughput.get().getCapacity();
        final double[] flowCapacities = new double[]{Float.MAX_VALUE, capacity,
                capacity / 2., capacity / 10.};
        // Sell the commodities.
        Builder[] soldComms = new Builder[FlowsCommonUtils.FLOW_KEYS.length];
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            soldComms[i] = buildSoldFlowComm(FlowsCommonUtils.FLOW_KEYS[i], flowCapacities[i]);
            pm.addCommoditySold(soldComms[i], Optional.empty());
        }
        // Get the VMs.
        Set<StitchingEntity> consumers = pm.getConsumers().stream().filter(e -> e.getEntityType()
                .equals(VIRTUAL_MACHINE)).collect(Collectors.toSet());
        consumers.forEach(c -> setFlowCommoditiesVMs(pm.getLocalId(), c, soldComms));
    }

    /**
     * Sets the flow commodities for VMs.
     *
     * @param providerID The provider ID.
     * @param vm         The entity (VM).
     * @param soldComms  The array of sold commodities.
     */
    private void setFlowCommoditiesVMs(final @Nonnull String providerID,
                                       final @Nonnull StitchingEntity vm,
                                       final @Nonnull Builder[] soldComms) {
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            CommodityBought comm = buildBoughtFlowComm(providerID, PHYSICAL_MACHINE, soldComms[i]);
            vm.getEntityBuilder().addCommoditiesBought(comm);
            vm.addCommoditySold(buildSoldFlowComm(FlowsCommonUtils.FLOW_KEYS[i],
                                                  soldComms[i].getCapacity()),
                                Optional.empty());
        }
        Set<StitchingEntity> consumers = vm.getConsumers().stream().filter(e -> e.getEntityType()
                .equals(CONTAINER_POD)).collect(Collectors.toSet());
        consumers.forEach(c -> setFlowCommoditiesPods(vm.getLocalId(), c, soldComms));
    }

    /**
     * Sets the flow commodities for Container Pods.
     *
     * @param providerID The provider ID.
     * @param vm         The entity (VM).
     * @param soldComms  The array of sold commodities.
     */
    private void setFlowCommoditiesPods(final @Nonnull String providerID,
                                        final @Nonnull StitchingEntity vm,
                                        final @Nonnull Builder[] soldComms) {
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            CommodityBought comm = buildBoughtFlowComm(providerID, VIRTUAL_MACHINE, soldComms[i]);
            vm.getEntityBuilder().addCommoditiesBought(comm);
        }
    }
}
