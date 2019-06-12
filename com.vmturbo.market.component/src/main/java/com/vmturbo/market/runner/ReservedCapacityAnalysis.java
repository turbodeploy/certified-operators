package com.vmturbo.market.runner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ReservedCapacityAnalysis {

    // This set contains all EntityTypes that may have reservation.
    private final static Set<Integer> reservedEntityType =
        ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.CONTAINER_VALUE);

    // Map from commBough commodityType to commSold commodityType.
    // For VM, if commBought is Cpu, then commSold is VCpu.
    // For VM, if commBought is Mem, then commSold is VMem.
    // For container, if commBought is VCpu, then commSold is VCpu.
    // For container, if commBought is VMem, then commSold is VMem.
    // This map is used to find the corresponding commSold given a commBought.
    private final static Map<Integer, Integer> commBoughtTypeToCommSoldType = ImmutableMap.of(
        CommodityDTO.CommodityType.CPU_VALUE, CommodityDTO.CommodityType.VCPU_VALUE,
        CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE,
        CommodityDTO.CommodityType.VCPU_VALUE, CommodityDTO.CommodityType.VCPU_VALUE,
        CommodityDTO.CommodityType.VMEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE);

    // Map from entityOid to TopologyEntityDTO.
    private final Map<Long, TopologyEntityDTO> oidToDto;

    // Map from a key consists of entityOid and commodityType to new reserved capacity.
    private final Map<String, Float> oidCommTypeToReserved;

    // A list contains all reservation resize actions.
    private final List<Action> actions;

    public ReservedCapacityAnalysis(@Nonnull final Map<Long, TopologyEntityDTO> topologyEntityDTOMap) {
        this.oidToDto = Objects.requireNonNull(topologyEntityDTOMap);
        oidCommTypeToReserved = new HashMap<>();
        actions = new ArrayList<>();
    }

    /**
     * This method is used to check if a reserved commodity needs to be resized.
     * If so, we will generate a resize action.
     */
    public void execute() {
        for (Entry<Long, TopologyEntityDTO> entry : oidToDto.entrySet()) {
            long oid = entry.getKey();
            TopologyEntityDTO entity = entry.getValue();
            if (!reservedEntityType.contains(entity.getEntityType()) ||
                !entity.getAnalysisSettings().getControllable()) {
                continue;
            }
            for (CommoditiesBoughtFromProvider commBoughtGrouping :
                entity.getCommoditiesBoughtFromProvidersList()) {
                if (!commBoughtGrouping.hasProviderId() ||
                    // No need to consider VMs added by Add Workload plan.
                    !oidToDto.containsKey(commBoughtGrouping.getProviderId()) || !oidToDto
                    .get(commBoughtGrouping.getProviderId()).getAnalysisSettings().getControllable()) {
                    continue;
                }
                for (CommodityBoughtDTO commBought : commBoughtGrouping.getCommodityBoughtList()) {
                    // Skip the commBought if its reservedCapacity is not greater than 0.
                    if (commBought.getReservedCapacity() <= 0) {
                        continue;
                    }
                    // Find the corresponding commSold given a commBought.
                    CommoditySoldDTO commSold = null;
                    for (CommoditySoldDTO c : entity.getCommoditySoldListList()) {
                        if (c.getCommodityType().getType() ==
                            commBoughtTypeToCommSoldType.get(commBought.getCommodityType().getType())) {
                            commSold = c;
                            break;
                        }
                    }
                    if (commSold == null || !commSold.getIsResizeable()) {
                        continue;
                    }
                    calculateReservedCapacity(oid, commBought, commSold);
                }
            }
        }
    }

    /**
     * Calculate the value that the commodity needs to be resized to.
     *
     * @param oid oid of the entity
     * @param commBought the commBought of the entity that may need to be resized
     * @param commSold the commSold of the entity corresponding to the commBought
     */
    private void calculateReservedCapacity(final long oid, final CommodityBoughtDTO commBought,
                                           final CommoditySoldDTO commSold) {
        double avgValue = commSold.hasHistoricalUsed() ?
            commSold.getHistoricalUsed().getMaxQuantity() : 0;
        double peakValue = commSold.hasHistoricalPeak() ?
            commSold.getHistoricalPeak().getMaxQuantity() : 0;
        double currentUsed = commSold.getUsed();
        double currentPeak = commSold.getPeak();
        avgValue = Math.max(currentUsed, avgValue);
        peakValue = Math.max(currentPeak, peakValue);
        peakValue = Math.max(avgValue, peakValue);

        double oldReservedCapacity = commBought.getReservedCapacity();
        final float usedIncrement = commSold.getCapacityIncrement();
        // Only consider resize down.
        if (peakValue < oldReservedCapacity) {
            // Set new reservation value to peak if peak is not 0.
            double newReservedCapacity = (peakValue == 0) ? oldReservedCapacity / 2 : peakValue;
            // Find out the increment change in reservation of commodity.
            double difference = Math.abs(oldReservedCapacity - newReservedCapacity);
            int rateOfDifference = (int) Math.floor(difference / usedIncrement);
            double result = rateOfDifference * usedIncrement;
            result = Math.min(result, oldReservedCapacity);

            if (result > 0 && result != oldReservedCapacity) {
                newReservedCapacity = oldReservedCapacity - result;
                oidCommTypeToReserved.put(composeKey(oid, commBought.getCommodityType()),
                    (float) newReservedCapacity);
                generateResizeReservationAction(oid, commBought, (float) newReservedCapacity);
            }
        }
    }

    /**
     * Generate a resize action for the commodity that needs to be resized.
     *
     * @param oid oid of the entity
     * @param commBought the commBought of the entity that needs to be resized
     * @param newReservedCapacity the value that the commodity needs to be resized to
     */
    private void generateResizeReservationAction(final long oid, CommodityBoughtDTO commBought,
                                                 final float newReservedCapacity) {
        final Explanation.Builder expBuilder = Explanation.newBuilder();
        // We use utilization only for deciding resize up or resize down.
        // Here we just use reservedCapacity as utilization.
        expBuilder.setResize(ResizeExplanation.newBuilder()
                                .setStartUtilization(newReservedCapacity)
                                .setEndUtilization((float) commBought.getReservedCapacity())
                                .build());

        final Action.Builder action = Action.newBuilder()
            // Assign a unique ID to each generated action.
            .setId(IdentityGenerator.next())
            // This action is generated out of market. There is no related revenue which can be used
            // to calculate importance. Just make the severity of this action MINOR.
            .setImportance(-1.0d)
            .setExplanation(expBuilder.build())
            .setExecutable(true);

        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                           .setId(oid)
                           .setType(oidToDto.get(oid).getEntityType())
                           .setEnvironmentType(oidToDto.get(oid).getEnvironmentType())
                           .build())
            .setNewCapacity(newReservedCapacity)
            .setOldCapacity((float) commBought.getReservedCapacity())
            .setCommodityType(commBought.getCommodityType())
            .setCommodityAttribute(CommodityAttribute.RESERVED);

        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        infoBuilder.setResize(resizeBuilder.build());

        action.setInfo(infoBuilder);
        actions.add(action.build());
    }

    /**
     * Get the value that the commodity needs to be resized to by oid and commodityType.
     *
     * @param oid oid of the entity
     * @param commodityType commodity type
     * @return the value that the commodity needs to be resized to
     */
    public float getReservedCapacity(final long oid, final CommodityType commodityType) {
        return oidCommTypeToReserved.getOrDefault(composeKey(oid, commodityType), 0.0f);
    }

    /**
     * Generate a unique key by entity oid and commodityType.
     *
     * @param oid oid of the entity
     * @param commodityType commodity type
     * @return the key of {@link ReservedCapacityAnalysis#oidCommTypeToReserved}
     */
    private String composeKey(final long oid, final CommodityType commodityType) {
        return oid+ "-" + commodityType.getType();
    }

    /**
     * Get all reservation resize actions.
     *
     * @return A Collection contains all reservation resize actions.
     */
    public Collection<Action> getActions() {
        return actions;
    }
}
