package com.vmturbo.topology.processor.ncm;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.FLOW;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.NET_THROUGHPUT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.*;

public class FlowCommoditiesGenerator {
    /**
     * The matrix.
     */
    private final MatrixInterface matrix;

    /**
     * Constructs the {@link FlowCommoditiesGenerator}
     *
     * @param matrix The Communication Matrix.
     */
    public FlowCommoditiesGenerator(final @Nonnull MatrixInterface matrix) {
        this.matrix = matrix;
    }

    /**
     * Generates Flow commodities.
     *
     * @param graph The graph with settings.
     */
    public void generateCommodities(final @Nonnull TopologyStitchingGraph graph) {
        // Get PMs.
        graph.entities().filter(e -> e.getEntityType().equals(PHYSICAL_MACHINE))
             .forEach(this::sellFlowCommoditiesPM);
        // Generate Underlay graph.
        generateUnderlayGraph(graph);
        // Populate DPoDs.
        graph.entities().filter(e -> e.getEntityType().equals(CHASSIS))
             .forEach(this::getNetworkDpods);
    }

    /**
     * Generates the underlay network, and places consumers on providers.
     *
     * @param graph The topology stitching graph.
     */
    private void generateUnderlayGraph(final @Nonnull TopologyStitchingGraph graph) {
        graph.entities().filter(e -> e.getEntityType().equals(AVAILABILITY_ZONE))
             .forEach(this::setAZTopology);
        graph.entities().filter(e -> e.getEntityType().equals(PHYSICAL_MACHINE))
             .forEach(this::setPMTopology);
        graph.entities().filter(e -> e.getEntityType().equals(VIRTUAL_MACHINE))
             .forEach(this::setVMTopology);
        graph.entities().filter(e -> e.getEntityType().equals(CONTAINER_POD))
             .forEach(this::setPodTopology);
    }

    /**
     * Obtains the datacenter for a physical host.
     *
     * @param pm The physical host.
     * @return The optional DC.
     */
    private Optional<StitchingEntity> getDCForPM(final @Nonnull TopologyStitchingEntity pm) {
        Optional<StitchingEntity> dc = pm.getProviders().stream()
                                         .filter(p -> p.getEntityType().equals(DATACENTER))
                                         .findFirst();
        if (!dc.isPresent()) {
            dc = pm.getProviders().stream().filter(p -> p.getEntityType().equals(CHASSIS))
                   .findFirst();
            return dc.flatMap(
                e -> e.getProviders().stream().filter(p -> p.getEntityType().equals(DATACENTER))
                      .findFirst());
        }
        return dc;
    }

    /**
     * Obtains the region for an availability zone.
     *
     * @param az The availability zone.
     * @return The optional Region.
     */
    private Optional<StitchingEntity> getRegionForAZ(final @Nonnull TopologyStitchingEntity az) {
        for (Set<StitchingEntity> set : az.getConnectedFromByType().values()) {
            Optional<StitchingEntity> region = set.stream()
                                                  .filter(p -> p.getEntityType().equals(REGION))
                                                  .findFirst();
            if (region.isPresent()) {
                return region;
            }
        }
        return Optional.empty();
    }

    /**
     * Sets up Availability Zone underlay network.
     *
     * @param az The availability zone.
     */
    private void setAZTopology(final @Nonnull TopologyStitchingEntity az) {
        final Optional<StitchingEntity> region = getRegionForAZ(az);
        region.ifPresent(stitchingEntity -> matrix.populateUnderlay(az.getOid(),
                                                                    stitchingEntity.getOid()));
    }

    /**
     * Sets up PM underlay network.
     *
     * @param pm The physical host.
     */
    private void setPMTopology(final @Nonnull TopologyStitchingEntity pm) {
        final Optional<StitchingEntity> dc = getDCForPM(pm);
        dc.ifPresent(stitchingEntity -> matrix.populateUnderlay(pm.getOid(),
                                                                stitchingEntity.getOid()));
    }

    /**
     * Sets up VM placement.
     *
     * @param vm The Virtual Machine.
     */
    private void setVMTopology(final @Nonnull TopologyStitchingEntity vm) {
        // Find all IP addresses and do the association.
        for (String ip : vm.getEntityBuilder().getVirtualMachineData().getIpAddressList()) {
            matrix.setEndpointOID(vm.getOid(), ip);
        }
        // Do the place.
        Optional<StitchingEntity> pm = vm.getProviders().stream()
                                         .filter(p -> p.getEntityType().equals(PHYSICAL_MACHINE))
                                         .findFirst();
        pm.ifPresent(stitchingEntity -> matrix.place(vm.getOid(), stitchingEntity.getOid()));
    }

    /**
     * Sets up Container Pod placement.
     *
     * @param pod The Container Pod.
     */
    private void setPodTopology(final @Nonnull TopologyStitchingEntity pod) {
        // Find all IP addresses and do the association.
        matrix.setEndpointOID(pod.getOid(),
                              pod.getEntityBuilder().getContainerPodData().getIpAddress());
        // Do the place.
        Optional<StitchingEntity> vm = pod.getProviders().stream()
                                          .filter(p -> p.getEntityType().equals(VIRTUAL_MACHINE))
                                          .findFirst();
        vm.ifPresent(stitchingEntity -> matrix.place(pod.getOid(), stitchingEntity.getOid()));
    }

    /**
     * Obtains network DPoDs.
     *
     * @param chassis The Chassis.
     */
    private void getNetworkDpods(final @Nonnull TopologyStitchingEntity chassis) {
        Set<Long> dpod = chassis.getConsumers().stream()
                                .filter(e -> e.getEntityType().equals(PHYSICAL_MACHINE))
                                .map(StitchingEntity::getOid).collect(Collectors.toSet());
        matrix.populateDpod(dpod);
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
                                     .setCommodityType(FLOW)
                                     .setActive(true)
                                     .setResizable(true)
                                     .setKey(key)
                                     .setCapacity(capacity);
    }

    /**
     * Build bought Flow commodity.
     *
     * @param providerID   The provider ID.
     * @param providerType The provider type.
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
                                            .filter(c -> c.getCommodityType()
                                                          .equals(NET_THROUGHPUT))
                                            .findFirst();
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
        Set<StitchingEntity> consumers = pm.getConsumers().stream()
                                           .filter(e -> e.getEntityType().equals(VIRTUAL_MACHINE))
                                           .collect(Collectors.toSet());
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
        Set<StitchingEntity> consumers = vm.getConsumers().stream()
                                           .filter(e -> e.getEntityType().equals(CONTAINER_POD))
                                           .collect(Collectors.toSet());
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

    //
    // Set commmodity capacities
    //

    /**
     * Sets the flow capacities.
     *
     * @param graph The topology graph.
     */
    public void setFlowCapacities(final @Nonnull TopologyGraph graph) {
        graph.entities().filter(e -> e.getEntityType() == PHYSICAL_MACHINE.getNumber())
             .forEach(this::setPMFlowCapacities);
    }

    /**
     * Sets the flow capacities for the PM.
     *
     * @param pm The physical host.
     */
    private void setPMFlowCapacities(final @Nonnull TopologyEntity pm) {
        double[] capacities = new double[FlowsCommonUtils.FLOW_KEYS.length];
        double[] utilThresholds = new double[FlowsCommonUtils.FLOW_KEYS.length];
        // Using local variable to assist with formatting down the line.
        for (TopologyDTO.CommoditySoldDTO comm : pm.getTopologyEntityDtoBuilder()
                                                   .getCommoditySoldListList()) {
            final String key = comm.getCommodityType().getKey();
            if (!key.startsWith(FlowsCommonUtils.KEY_PREFIX)) {
                continue;
            }
            int index = Integer.parseInt(
                comm.getCommodityType().getKey().substring(key.length() - 1));
            capacities[index] = comm.getCapacity();
            utilThresholds[index] = Math.ceil(comm.getEffectiveCapacityPercentage() / 100D);
        }
        matrix.setCapacities(pm.getOid(), capacities, utilThresholds);
    }
}
