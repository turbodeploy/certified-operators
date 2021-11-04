package com.vmturbo.market.runner.reservedcapacity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class to manage generation of reservations.
 */
public class ReservedCapacityAnalysisEngine {

    // This set contains all EntityTypes that may have reservation.
    private static final Set<Integer> reservedEntityType =
        ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.CONTAINER_VALUE);

    // Map from commBough commodityType to commSold commodityType.
    // For VM, if commBought is Cpu, then commSold is VCpu.
    // For VM, if commBought is Mem, then commSold is VMem.
    // For container, if commBought is VCpu, then commSold is VCpu.
    // For container, if commBought is VMem, then commSold is VMem.
    // This map is used to find the corresponding commSold given a commBought.
    private static final Map<Integer, Integer> commBoughtTypeToCommSoldType = ImmutableMap.of(
        CommodityDTO.CommodityType.CPU_VALUE, CommodityDTO.CommodityType.VCPU_VALUE,
        CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE,
        CommodityDTO.CommodityType.VCPU_VALUE, CommodityDTO.CommodityType.VCPU_VALUE,
        CommodityDTO.CommodityType.VMEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE);

    /**
     * Helper class to capture objects required during analysis. Used to avoid super long
     * helper method signatures.
     */
    private static class AnalysisContext {

        private final Map<Long, TopologyEntityDTO> oidToDto;

        private final ConsistentScalingHelper consistentScalingHelper;

        private final Map<String, ReservationGroup> reservationGroups = new HashMap<>();

        /**
         * In-progress results. We add actions to this results object as we do the analysis.
         */
        private final ReservedCapacityResults results = new ReservedCapacityResults();

        private AnalysisContext(Map<Long, TopologyEntityDTO> oidToDto,
                ConsistentScalingHelper consistentScalingHelper) {
            this.oidToDto = oidToDto;
            this.consistentScalingHelper = consistentScalingHelper;
        }

        Optional<String> getScalingGroupId(long oid) {
            return Optional.ofNullable(consistentScalingHelper)
                    .flatMap(scalingHelper -> scalingHelper.getScalingGroupId(oid));
        }
    }

    /**
     * This method is used to check if a reserved commodity needs to be resized.
     * If so, we will generate a resize action.
     *
     * @param oidToDto The topology to analyze.
     * @param consistentScalingHelper consistent scaling helper, used to identify scaling groups.
     * @return The {@link ReservedCapacityResults}.
     */
    @Nonnull
    public ReservedCapacityResults execute(@Nonnull final Map<Long, TopologyEntityDTO> oidToDto,
                                           @Nonnull final ConsistentScalingHelper consistentScalingHelper) {
        // Tracks groups of reservations for scaling group members
        AnalysisContext context = new AnalysisContext(oidToDto, consistentScalingHelper);
        for (Entry<Long, TopologyEntityDTO> entry : oidToDto.entrySet()) {
            long oid = entry.getKey();
            TopologyEntityDTO entity = entry.getValue();
            if (!reservedEntityType.contains(entity.getEntityType())
                    || !entity.getAnalysisSettings().getControllable()
                    || entity.getEntityState() != EntityState.POWERED_ON) {
                continue;
            }
            for (CommoditiesBoughtFromProvider commBoughtGrouping : entity.getCommoditiesBoughtFromProvidersList()) {
                if (!commBoughtGrouping.hasProviderId()
                        || // No need to consider VMs added by Add Workload plan.
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
                        if (c.getCommodityType().getType()
                                == commBoughtTypeToCommSoldType.get(commBought.getCommodityType().getType())) {
                            commSold = c;
                            break;
                        }
                    }
                    // We want to avoid resizing reservation when it is equal to or greater than
                    // the capacity of the VM as there may be some misconfiguration or reservation
                    // locking where the customer doesn't want reservation resizes since they
                    // want it to remain locked to capacity. This will take precedence over
                    // consistent scaling and this change will prevent this entity's tentative
                    // reservation value from being considered when calculating the max reservation
                    // for a scaling group.
                    // TODO: Mediation should expose the reservation lock value on an entity
                    if (commSold == null || !commSold.hasCapacity()
                                    || (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) && (!commSold.getIsResizeable()
                                                    || (commBought.getReservedCapacity() - commSold.getCapacity()
                                    >= -MarketAnalysisUtils.EPSILON))) {
                        continue;
                    }
                    calculateReservedCapacity(oid, commBought, commSold, context);
                }
            }
        }
        // Generate reservations for scaling groups.
        context.reservationGroups.values().forEach(rg -> rg.addReservationsToResults(context));
        return context.results;
    }

    /**
     * Calculate the value that the commodity needs to be resized to.
     *
     * @param oid oid of the entity
     * @param commBought the commBought of the entity that may need to be resized
     * @param commSold the commSold of the entity corresponding to the commBought
     * @param analysisContext Context holding relevant state objects for the analysis.
     */
    private void calculateReservedCapacity(final long oid, final CommodityBoughtDTO commBought,
                                           final CommoditySoldDTO commSold,
                                           AnalysisContext analysisContext) {
        double avgValue = commSold.hasHistoricalUsed()
                ? commSold.getHistoricalUsed().getMaxQuantity() : 0;
        double peakValue = commSold.hasHistoricalPeak()
                ? commSold.getHistoricalPeak().getMaxQuantity() : 0;
        double currentUsed = commSold.getUsed();
        double currentPeak = commSold.getPeak();
        avgValue = Math.max(currentUsed, avgValue);
        peakValue = Math.max(currentPeak, peakValue);
        peakValue = Math.max(avgValue, peakValue);

        double oldReservedCapacity = commBought.getReservedCapacity();
        final float usedIncrement = commSold.getCapacityIncrement();
        double newReservedCapacity = 0;
        // Only consider resize down. Always process scaling group members. Scaling group members
        // that are idle or powered off are not processed, because they are ommited from the
        // scaling group.
        Optional<String> scalingGroupId = analysisContext.getScalingGroupId(oid);
        if (peakValue < oldReservedCapacity || scalingGroupId.isPresent()) {
            // Set new reservation value to peak if peak is not 0.
            newReservedCapacity = (peakValue == 0) ? oldReservedCapacity / 2 : peakValue;
            // Consider the capacity lower bound threshold when making the resizes.
            if (oldReservedCapacity > commSold.getThresholds().getMin()
                    && newReservedCapacity < commSold.getThresholds().getMin()) {
                newReservedCapacity = commSold.getThresholds().getMin();
            }
            // Find out the increment change in reservation of commodity.
            double difference = Math.abs(oldReservedCapacity - newReservedCapacity);
            int rateOfDifference = (int)Math.floor(difference / usedIncrement);
            double result = rateOfDifference * usedIncrement;
            result = Math.min(result, oldReservedCapacity);
            if (scalingGroupId.isPresent()) {
                String key = scalingGroupId.get() + ":" + commBought.getCommodityType().getType();
                ReservationGroup rg = analysisContext.reservationGroups.computeIfAbsent(key,
                        k -> new ReservationGroup(scalingGroupId, usedIncrement));
                rg.addReservation(oid, commBought, (float)newReservedCapacity);
            } else if (result > 0 && result != oldReservedCapacity) {
                newReservedCapacity = oldReservedCapacity - result;
                addResizeReservationActionToResults(scalingGroupId, oid, commBought,
                    (float)newReservedCapacity, analysisContext);
            }
        }
    }

    /**
     * Generate a resize action for the commodity that needs to be resized.
     *
     * @param oid oid of the entity
     * @param scalingGroupId scaling group ID, if the OID is a member of a scaling group
     * @param commBought the commBought of the entity that needs to be resized
     * @param newReservedCapacity the value that the commodity needs to be resized to
     * @param analysisContext Analysis context. Modified inside this method (since the generated
     *                        action gets added to the results).
     */
    private void addResizeReservationActionToResults(final Optional<String> scalingGroupId,
                                                 final long oid, CommodityBoughtDTO commBought,
                                                 final float newReservedCapacity,
                                                 final AnalysisContext analysisContext) {
        final Explanation.Builder expBuilder = Explanation.newBuilder();
        // We use utilization only for deciding resize up or resize down.
        // Here we just use reservedCapacity as utilization.
        ResizeExplanation.Builder resizeExplanation = ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(newReservedCapacity)
                .setDeprecatedEndUtilization((float)commBought.getReservedCapacity());
        scalingGroupId.ifPresent(resizeExplanation::setScalingGroupId);
        expBuilder.setResize(resizeExplanation);

        final Action.Builder action = Action.newBuilder()
            // Assign a unique ID to each generated action.
            .setId(IdentityGenerator.next())
            // This action is generated out of market. There is no related revenue which can be used
            // to calculate importance. Just make the severity of this action MINOR.
            .setDeprecatedImportance(-1.0d)
            .setExplanation(expBuilder.build())
            .setExecutable(true);

        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                           .setId(oid)
                           .setType(analysisContext.oidToDto.get(oid).getEntityType())
                           .setEnvironmentType(analysisContext.oidToDto.get(oid).getEnvironmentType())
                           .build())
            .setNewCapacity(newReservedCapacity)
            .setOldCapacity((float)commBought.getReservedCapacity())
            .setCommodityType(commBought.getCommodityType())
            .setCommodityAttribute(CommodityAttribute.RESERVED);
        scalingGroupId.ifPresent(resizeBuilder::setScalingGroupId);

        final ActionInfo.Builder infoBuilder = ActionInfo.newBuilder();
        infoBuilder.setResize(resizeBuilder.build());

        action.setInfo(infoBuilder);

        analysisContext.results.putReservedCapacity(oid, commBought.getCommodityType(), newReservedCapacity);
        analysisContext.results.addAction(action.build());
    }

    /**
     * This tracks all pending reservations within a scaling group on a per-commodity basis.
     */
    class ReservationGroup {
        private Optional<String> scalingGroupId_;
        private float maxReservation_;
        // Since all scaling group members share the same policies/settings, the used increment
        // setting will be the same for all of the members, so we only need to save one copy.
        private float usedIncrement_;
        private List<TentativeReservation> reservations_;

        ReservationGroup(Optional<String> scalingGroupId, final float usedIncrement) {
            this.scalingGroupId_ = scalingGroupId;
            this.maxReservation_ = 0;
            this.usedIncrement_ = usedIncrement;
            this.reservations_ = new ArrayList<>();
        }

        /**
         * Add a provisional reservation for the given commodity bought.  These will all be stored
         * until after all entities in the scaling group have been processed.
         * @param oid OID of entity.
         * @param commBought commodity bought to resize
         * @param newReservedCapacity new reservation
         */
        public void addReservation(Long oid, CommodityBoughtDTO commBought,
                                   float newReservedCapacity) {
            this.maxReservation_ = Math.max(this.maxReservation_, newReservedCapacity);
            this.reservations_.add(new TentativeReservation(oid, commBought));
        }

        /**
         * Generate reservations for the commodity bought in this reservation group.  Only resize
         * down events are generated.  If the scaling group started with mismatched reservations
         * for this commodity, we will refuse to consistently scale them if that means generating
         * a resize up action.
         *
         * @param context The analysis context. The results builder inside the context is modified
         *                as part of running this method.
         */
        public void addReservationsToResults(@Nonnull final AnalysisContext context) {
            if (maxReservation_ > 0) {
                float actionThreshold = maxReservation_ + usedIncrement_;
                reservations_.stream()
                    // Do not generate a reservation if the change is less than the used increment
                    // or if it's a resize up.
                    .filter(tr -> actionThreshold <= tr.commBought_.getReservedCapacity())
                    .forEach(tr -> addResizeReservationActionToResults(scalingGroupId_, tr.oid_,
                            tr.commBought_, maxReservation_, context));
            }
        }

        /**
         * Holds parameters for a tentative reservation request.
         */
        class TentativeReservation {
            private Long oid_;
            private CommodityBoughtDTO commBought_;

            TentativeReservation(Long oid, CommodityBoughtDTO commodityBoughtDTO) {
                this.oid_ = oid;
                this.commBought_ = commodityBoughtDTO;
            }
        }
    }
}
