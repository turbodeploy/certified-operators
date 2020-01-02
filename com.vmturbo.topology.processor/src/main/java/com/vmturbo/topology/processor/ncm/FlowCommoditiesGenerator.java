package com.vmturbo.topology.processor.ncm;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.FLOW;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.NET_THROUGHPUT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;

/**
 * The flow commodities generator.
 */
public class FlowCommoditiesGenerator {
    /**
     * The k8s flow capacities.
     */
    private static final double[] K8S_FLOW_CAPACITIES = new double[]{Float.MAX_VALUE, 1000000f,
                                                                     500000f, 100000f};

    /**
     * The matrix.
     */
    private final MatrixInterface matrix;

    /**
     * Constructs the {@link FlowCommoditiesGenerator}.
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
        // Generate Underlay graph.
        generateUnderlayGraph(graph);
        // Populate DPoDs.
        graph.entities().filter(EntityType::isChassis).forEach(this::getNetworkDpods);
        // Handle placement of Pods on Vms
        final List<StitchingEntity> pods = graph.entities().filter(EntityType::isContainerPod)
                                                .collect(Collectors.toList());
        containersInProvider(pods).forEach((k, v) -> v.forEach(pod -> matrix.place(pod, k)));
        // We've got PoD VMs. Now - group into DPODs.
        // DPoD in this case equals to cluster. This is determined by the ClusterCommodity key.
        containerDpods(pods).forEach(matrix::populateDpod);
        // Handle PMs and VMs.
        graph.entities().filter(EntityType::isPM).forEach(this::sellFlowCommoditiesPM);
        // Handle ContainerPODs.
        graph.entities().filter(EntityType::isContainerPod).forEach(this::handlePodFlows);
    }

    /**
     * Constructs the DPODs for Containers.
     * <br>
     * Returns a map of {@code k8s node oid, list of pods on that node}.
     *
     * @param pods The container pods in the graph.
     * @return The pods per node map.
     */
    private Map<Long, List<Long>> containersInProvider(@Nonnull final List<StitchingEntity> pods) {
        final Map<Long, List<Long>> pods4vm = new HashMap<>();
        for (StitchingEntity pod : pods) {
            Optional<StitchingEntity> vm = pod.getProviders().stream().filter(EntityType::isVM)
                                              .findFirst();
            if (!vm.isPresent()) {
                continue;
            }
            pods4vm.computeIfAbsent(vm.get().getOid(), k -> new ArrayList<>()).add(pod.getOid());
        }
        return pods4vm;
    }

    /**
     * Constructs the DPODs for Containers.
     * <br>
     * Returns a collection of {@code list of pods on that node}.
     *
     * @param pods The container pods in the graph.
     * @return The DPODs for Containers.
     */
    private Collection<Set<Long>> containerDpods(@Nonnull final List<StitchingEntity> pods) {
        final Map<String, Set<Long>> dpods = new HashMap<>();
        for (StitchingEntity pod : pods) {
            Optional<StitchingEntity> vmO = pod.getProviders().stream().filter(EntityType::isVM)
                                               .findFirst();
            if (!vmO.isPresent()) {
                continue;
            }
            StitchingEntity vm = vmO.get();
            Optional<Builder> clusterComm;
            clusterComm = vm.getCommoditiesSold()
                            .filter(c -> c.getCommodityType()
                                          .equals(CommonDTO.CommodityDTO.CommodityType.CLUSTER))
                            .findFirst();
            if (!clusterComm.isPresent() || clusterComm.get().getKey().trim().isEmpty()) {
                continue;
            }
            dpods.computeIfAbsent(clusterComm.get().getKey(), k -> new HashSet<>()).add(vm.getOid());
        }
        return dpods.values();
    }

    /**
     * Generates the underlay network, and places consumers on providers.
     *
     * @param graph The topology stitching graph.
     */
    private void generateUnderlayGraph(final @Nonnull TopologyStitchingGraph graph) {
        matrix.resetUnderlay();
        graph.entities().filter(EntityType::isAZ).forEach(this::setAZTopology);
        graph.entities().filter(EntityType::isPM).forEach(this::setPMTopology);
        graph.entities().filter(EntityType::isVM).forEach(this::setVMTopology);
        graph.entities().filter(EntityType::isContainerPod).forEach(this::setPodTopology);
    }

    /**
     * Obtains the datacenter for a physical host.
     *
     * @param pm The physical host.
     * @return The optional DC.
     */
    private Optional<StitchingEntity> getDCForPM(final @Nonnull StitchingEntity pm) {
        Optional<StitchingEntity> dc = pm.getProviders().stream().filter(EntityType::isDC)
                                         .findFirst();
        if (!dc.isPresent()) {
            dc = pm.getProviders().stream().filter(EntityType::isChassis).findFirst();
            return dc.flatMap(e -> e.getProviders().stream().filter(EntityType::isDC).findFirst());
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
        return az.getConnectedFromByType()
                 .getOrDefault(ConnectionType.OWNS_CONNECTION, Collections.emptySet())
                 .stream().filter(EntityType::isRegion).findAny();
    }

    /**
     * Sets up Availability Zone underlay network.
     *
     * @param az The availability zone.
     */
    private void setAZTopology(final @Nonnull TopologyStitchingEntity az) {
        getRegionForAZ(az).ifPresent(e -> matrix.populateUnderlay(az.getOid(), e.getOid()));
    }

    /**
     * Sets up PM underlay network.
     *
     * @param pm The physical host.
     */
    private void setPMTopology(final @Nonnull TopologyStitchingEntity pm) {
        getDCForPM(pm).ifPresent(e -> matrix.populateUnderlay(pm.getOid(), e.getOid()));
    }

    /**
     * Sets up VM placement.
     *
     * @param vm The Virtual Machine.
     */
    private void setVMTopology(final @Nonnull TopologyStitchingEntity vm) {
        Optional<StitchingEntity> pm = vm.getProviders().stream().filter(EntityType::isPM)
                                         .findFirst();
        // No parent, skip.
        if (!pm.isPresent()) {
            return;
        }
        // This is a kubernetes node, it may only live in the underlay network.
        if (isK8sNode(vm)) {
            getDCForPM(pm.get()).ifPresent(e -> matrix.populateUnderlay(vm.getOid(), e.getOid()));
            return;
        }
        // Find all IP addresses and do the association.
        for (String ip : vm.getEntityBuilder().getVirtualMachineData().getIpAddressList()) {
            matrix.setEndpointOID(vm.getOid(), ip);
        }
        // Do the place.
        matrix.place(vm.getOid(), pm.get().getOid());
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
        Optional<StitchingEntity> vm = pod.getProviders().stream().filter(EntityType::isVM)
                                          .findFirst();
        vm.ifPresent(stitchingEntity -> matrix.place(pod.getOid(), stitchingEntity.getOid()));
    }

    /**
     * Obtains network DPoDs.
     *
     * @param chassis The Chassis.
     */
    private void getNetworkDpods(final @Nonnull TopologyStitchingEntity chassis) {
        Set<Long> dpod = chassis.getConsumers().stream().filter(EntityType::isPM)
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
                                     .setUsed(0)
                                     .setCapacity(capacity);
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
        List<StitchingEntity> vms = new ArrayList<>();
        // Get the VDCs.
        pm.getConsumers().stream().filter(EntityType::isVDC)
          .forEach(vdc -> vdc.getConsumers().stream()
                             .filter(EntityType::isVM)
                             .collect(Collectors.toCollection(() -> vms)));
        // Get the VMs.
        pm.getConsumers().stream().filter(EntityType::isVM)
          .collect(Collectors.toCollection(() -> vms));
        // Skip if we are a kubernetes node.
        vms.stream().filter(this::isNotK8sNode)
           .forEach(c -> setFlowCommoditiesVMs(pm.getLocalId(), c, soldComms));
    }

    /**
     * Checks whether a VM is a Kubernetes node.
     *
     * @param vm The VM in question.
     * @return {@code true} iff VM is a k8s node.
     */
    private boolean isK8sNode(final @Nonnull StitchingEntity vm) {
        return vm.getConsumers().stream().anyMatch(EntityType::isContainerPod);
    }

    /**
     * Checks whether a VM is a Kubernetes node.
     *
     * @param vm The VM in question.
     * @return {@code true} iff VM is a k8s node.
     */
    private boolean isK8sNode(final @Nonnull TopologyEntity vm) {
        return vm.getConsumers().stream().anyMatch(EntityType::isContainerPod);
    }

    /**
     * Checks whether a VM is not a Kubernetes node.
     *
     * @param vm The VM in question.
     * @return {@code true} iff VM is not a k8s node.
     */
    private boolean isNotK8sNode(final @Nonnull StitchingEntity vm) {
        return !isK8sNode(vm);
    }

    /**
     * Constructs bought flow commodity.
     *
     * @param key      The key.
     * @param capacity The capacity.
     * @param used     The used value.
     * @return The bought commodity builder.
     */
    private @Nonnull CommonDTO.CommodityDTO.Builder genBoughtComm(final @Nonnull String key,
                                                                  final double used,
                                                                  final double capacity) {
        return CommonDTO.CommodityDTO.newBuilder().setCommodityType(FLOW).setActive(true)
                                     .setResizable(true).setKey(key).setUsed(used)
                                     .setCapacity(capacity);
    }

    /**
     * Retrieves the endpoint flows.
     *
     * @param enpoint The endpoint.
     * @return The flows.
     */
    private double[] getFlows(final @Nonnull StitchingEntity enpoint) {
        return matrix.getEndpointFlows(enpoint.getOid());
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
        // Check whether we've been here already.
        if (vm.getCommoditiesSold().anyMatch(c -> c.getKey().startsWith("FLOW"))) {
            return;
        }
        CommodityBought.Builder cb = CommodityBought.newBuilder().setProviderId(providerID)
                                                    .setProviderType(PHYSICAL_MACHINE);
        final double[] flows = getFlows(vm);
        if (flows.length == 0) {
            return;
        }
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            cb.addBought(
                genBoughtComm(soldComms[i].getKey(), flows[i], soldComms[i].getCapacity()));
            vm.addCommoditySold(buildSoldFlowComm(FlowsCommonUtils.FLOW_KEYS[i],
                                                  soldComms[i].getCapacity()),
                                Optional.empty());
        }
        addBoughtComm(vm, PHYSICAL_MACHINE, cb);
    }

    /**
     * Add bought commodities.
     *
     * @param se   The entity.
     * @param type The provider entity type.
     * @param cb   The bought commodities.
     */
    private void addBoughtComm(final @Nonnull StitchingEntity se,
                               final @Nonnull CommonDTO.EntityDTO.EntityType type,
                               final @Nonnull CommodityBought.Builder cb) {
        List<CommonDTO.CommodityDTO.Builder> comms = cb.getBoughtList().stream()
                                                       .map(CommonDTO.CommodityDTO::toBuilder)
                                                       .collect(Collectors.toList());
        for (Map.Entry<StitchingEntity, List<CommoditiesBought>> e :
            se.getCommodityBoughtListByProvider().entrySet()) {
            if (e.getKey().getEntityBuilder().getEntityType() == type) {
                final List<CommoditiesBought> v = e.getValue();
                if (v.isEmpty()) {
                    v.add(new CommoditiesBought(comms));
                } else {
                    v.get(v.size() - 1).getBoughtList().addAll(comms);
                }
                return;
            }
        }
    }

    /**
     * Sets the flow commodities for K8s Node VMs.
     *
     * @param vm The entity (VM).
     */
    private void setFlowCommoditiesK8sVMs(final @Nonnull StitchingEntity vm) {
        // Check whether we've been here already.
        if (vm.getCommoditiesSold().anyMatch(c -> c.getKey().startsWith("FLOW"))) {
            return;
        }
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            vm.addCommoditySold(buildSoldFlowComm(FlowsCommonUtils.FLOW_KEYS[i],
                                                  K8S_FLOW_CAPACITIES[i]),
                                Optional.empty());
        }
    }

    /**
     * Handle ContainerPod flows.
     *
     * @param pod The container PODs.
     */
    private void handlePodFlows(final @Nonnull StitchingEntity pod) {
        Optional<StitchingEntity> vm = pod.getProviders().stream().filter(EntityType::isVM)
                                          .findFirst();
        // Handle Pods with a proper provider.
        if (vm.isPresent()) {
            setFlowCommoditiesK8sVMs(vm.get());
            setFlowCommoditiesPods(vm.get().getLocalId(), pod);
        }
    }

    /**
     * Sets the flow commodities for Container Pods.
     *
     * @param providerID The provider ID.
     * @param pod        The entity.
     */
    private void setFlowCommoditiesPods(final @Nonnull String providerID,
                                        final @Nonnull StitchingEntity pod) {
        CommodityBought.Builder cb = CommodityBought.newBuilder()
                                                    .setProviderId(providerID)
                                                    .setProviderType(VIRTUAL_MACHINE);
        final double[] flows = getFlows(pod);
        if (flows.length == 0) {
            return;
        }
        for (int i = 0; i < FlowsCommonUtils.FLOW_KEYS.length; i++) {
            cb.addBought(genBoughtComm(FlowsCommonUtils.FLOW_KEYS[i],
                                       flows[i], K8S_FLOW_CAPACITIES[i]));
        }
        addBoughtComm(pod, VIRTUAL_MACHINE, cb);
    }

    //
    // Set commmodity capacities
    //

    /**
     * Sets the flow capacities.
     *
     * @param graph The topology graph.
     */
    public void setFlowCapacities(final @Nonnull TopologyGraph<TopologyEntity> graph) {
        graph.entities().filter(e -> e.getEntityType() == PHYSICAL_MACHINE.getNumber())
             .forEach(this::setUnderlayNodeFlowCapacities);
        graph.entities().filter(e -> e.getEntityType() == VIRTUAL_MACHINE.getNumber())
             .filter(this::isK8sNode).forEach(this::setUnderlayNodeFlowCapacities);
    }

    /**
     * Sets the flow capacities for an underlay nodes.
     *
     * @param underlayNode The underlay node.
     */
    private void setUnderlayNodeFlowCapacities(final @Nonnull TopologyEntity underlayNode) {
        final double[] capacities = new double[FlowsCommonUtils.FLOW_KEYS.length];
        final double[] utilThresholds = new double[FlowsCommonUtils.FLOW_KEYS.length];
        // Using local variable to assist with formatting down the line.
        for (TopologyDTO.CommoditySoldDTO comm : underlayNode.getTopologyEntityDtoBuilder()
                                                             .getCommoditySoldListList()) {
            final String key = comm.getCommodityType().getKey();
            if (!key.startsWith(FlowsCommonUtils.KEY_PREFIX)) {
                continue;
            }
            final int index = Integer.parseInt(comm.getCommodityType().getKey()
                                                   .substring(key.length() - 1));
            capacities[index] = comm.getCapacity();
            utilThresholds[index] = Math.ceil(comm.getEffectiveCapacityPercentage() / 100D);
        }
        matrix.setCapacities(underlayNode.getOid(), capacities, utilThresholds);
    }
}
