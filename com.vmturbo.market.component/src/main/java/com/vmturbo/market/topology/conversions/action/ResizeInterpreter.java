package com.vmturbo.market.topology.conversions.action;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.CpuScalingPolicy;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Responsible for turning a {@link ResizeTO} generated by the market into a {@link Resize} and
 * {@link ResizeExplanation} expected by the rest of the XL platform.
 */
public class ResizeInterpreter extends ActionInterpretationAdapter<ResizeTO, Resize, ResizeExplanation> {

    private final Map<Long, TopologyEntityDTO> originalTopology;

    /**
     * Create a new interpreter in the context of a specific topology.
     *
     * @param commodityConverter The {@link CommodityConverter}.
     * @param commodityIndex THe {@link CommodityIndex}.
     * @param oidToProjectedTraderTOMap The {@link TraderTO}s arranged by oid.
     * @param originalTopology The {@link TopologyEntityDTO}s in the original topology
     */
    public ResizeInterpreter(@Nonnull final CommodityConverter commodityConverter,
                             @Nonnull final CommodityIndex commodityIndex,
                             @Nonnull final Map<Long, TraderTO> oidToProjectedTraderTOMap,
                             @Nonnull final Map<Long, TopologyEntityDTO> originalTopology) {
        super(commodityConverter, commodityIndex, oidToProjectedTraderTOMap);
        this.originalTopology = originalTopology;
    }

    @Nonnull
    @Override
    public Optional<Resize> interpret(@Nonnull final ResizeTO resizeTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology) {
        final long entityId = resizeTO.getSellingTrader();
        final CommoditySpecificationTO cs = resizeTO.getSpecification();
        final CommodityType topologyCommodityType =
                commodityConverter.marketToTopologyCommodity(cs)
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Resize commodity can't be converted to topology commodity format! "
                                        + cs));
        // Determine if this is a remove limit or a regular resize.
        final TopologyEntityDTO projectedEntity = projectedTopology.get(entityId).getEntity();

        // Find the CommoditySoldDTO for the resize commodity.
        final Optional<CommoditySoldDTO> resizeCommSold =
                projectedEntity.getCommoditySoldListList().stream()
                        .filter(commSold -> commSold.getCommodityType().equals(topologyCommodityType))
                        .findFirst();
        final Optional<CommoditySoldDTO> originalCommoditySold =
                commodityIndex.getCommSold(projectedEntity.getOid(), topologyCommodityType);
        double oldCapacity = TopologyConverter.reverseScaleComm(resizeTO.getOldCapacity(), originalCommoditySold, CommoditySoldDTO::getScalingFactor);
        double newCapacity = TopologyConverter.reverseScaleComm(resizeTO.getNewCapacity(), originalCommoditySold, CommoditySoldDTO::getScalingFactor);
        Pair<Integer, Integer> cpsrChanges = null;
        // Convert VCPU from MHz to cores for virtual machine
        if (projectedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && topologyCommodityType.getType() == CommodityDTO.CommodityType.VCPU_VALUE) {
            final TopologyEntityDTO originalEntity = originalTopology.get(entityId);
            if (originalEntity == null) {
                return Optional.empty();
            }
            // We don't have to find the sold commodities on the original entities, because it
            // should be identical to the "old" capacity.
            final VirtualMachineInfo vmInfo =
                            originalEntity.getTypeSpecificInfo().getVirtualMachine();

            final int numCores = Math.max(vmInfo.getNumCpus(), 1);

            final double cpuSpeedMhz = oldCapacity / numCores;
            if (cpuSpeedMhz == 0.0) {
                // If we can't get the CPU speed we can't interpret the resize, so discard it.
                return Optional.empty();
            }
            oldCapacity = Math.round(oldCapacity / cpuSpeedMhz);
            // First round to the 2nd decimal places and then do ceiling to handle cases like
            // 14400.523 / 2400 = 6.0002179167
            newCapacity = Math.ceil((int)(newCapacity / cpuSpeedMhz * 100) / 100.0f);
            cpsrChanges = getCpsrChanges(vmInfo, newCapacity);
            // double comparison normally applies epsilon, but in this case both capacities are
            // result of Math.round and Math.ceil so the values are actually integers.
            if (Double.compare(oldCapacity, newCapacity) == 0) {
                // If the from and to end up being the same number of CPUs, this action is not
                // really meaningful.
                return Optional.empty();
            }
        }

        final Resize removeLimit = interpretRemoveLimit(projectedEntity, resizeTO, topologyCommodityType, resizeCommSold);
        if (removeLimit != null) {
            return Optional.of(removeLimit);
        }

        if (!validResizeTrigger(oldCapacity, newCapacity, resizeTO)) {
            return Optional.empty();
        }

        final ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
                .setTarget(createActionEntity(entityId, projectedEntity.getEntityType(), projectedEntity.getEnvironmentType()))
                .setNewCapacity((float)newCapacity)
                .setOldCapacity((float)oldCapacity)
                .setCommodityType(topologyCommodityType);
        if (cpsrChanges != null) {
            resizeBuilder.setOldCpsr(cpsrChanges.getFirst());
            resizeBuilder.setNewCpsr(cpsrChanges.getSecond());
        }
        setHotAddRemove(resizeBuilder, resizeCommSold);
        if (resizeTO.hasScalingGroupId()) {
            resizeBuilder.setScalingGroupId(resizeTO.getScalingGroupId());
        }
        return Optional.of(resizeBuilder.build());
    }

    @Nullable
    private static Pair<Integer, Integer> getCpsrChanges(@Nonnull VirtualMachineInfo vmInfo,
                    double newCapacity) {
        if (vmInfo.hasCpuScalingPolicy()) {
            final CpuScalingPolicy cpuScalingPolicy = vmInfo.getCpuScalingPolicy();
            if (cpuScalingPolicy.hasSockets()) {
                return Pair.create(vmInfo.getCoresPerSocketRatio(),
                                (int)(Math.ceil(newCapacity / cpuScalingPolicy.getSockets())));
            }
        }
        return null;
    }

    @Nullable
    private Resize interpretRemoveLimit(@Nonnull final TopologyEntityDTO projectedEntity,
            @Nonnull final ResizeTO resize,
            @Nonnull final CommodityType topologyCommodityType,
            final Optional<CommoditySoldDTO> resizeCommSold) {
        // If this is a VM and has a restricted capacity, we are going to assume it's a limit
        // removal. This logic seems like it could be fragile, in that limit may not be the
        // only way VM capacity could be restricted in the future, but this is consistent
        // with how classic makes the same decision.
        if (projectedEntity.getEntityType() == EntityType.VIRTUAL_MACHINE.getNumber()) {
            TraderTO traderTO = oidToProjectedTraderTOMap.get(projectedEntity.getOid());
            // find the commodity on the trader and see if there is a limit?
            for (CommoditySoldTO commoditySold : traderTO.getCommoditiesSoldList()) {
                if (commoditySold.getSpecification().equals(resize.getSpecification())) {
                    // We found the commodity sold.  If it has a utilization upper bound < 1.0,
                    // then the commodity is restricted, and according to our VM-rule, we will
                    // treat this as a limit removal.
                    float utilizationPercentage = commoditySold.getSettings().getUtilizationUpperBound();
                    if (utilizationPercentage < 1.0) {
                        // The "limit removal" is actually a resize on the commodity's "limit"
                        // attribute that effectively sets it to zero.
                        //
                        // Ideally we would set the "old capacity" to the current limit
                        // value, but as noted above, we don't have access to the limit here. We
                        // only have the utilization percentage, which we expect to be based on
                        // the limit and raw capacity values. Since we do have the utilization %
                        // and raw capacity here, we can _approximate_ the current limit by
                        // reversing the math used to determine the utilization %.
                        //
                        // We will grudgingly do that here. Note that this may be subject to
                        // precision and rounding errors. In addition, if in the future we have
                        // factors other than "limit" that could drivef the VM resource
                        // utilization threshold to below 100%, then this approximation would
                        // likely be wrong and misleading in those cases.
                        float approximateLimit = commoditySold.getCapacity() * utilizationPercentage;
                        logger.debug("The commodity {} has util% of {}, so treating as limit"
                                        + " removal (approximate limit: {}).",
                                topologyCommodityType.getKey(), utilizationPercentage, approximateLimit);

                        ActionDTO.Resize.Builder resizeBuilder = ActionDTO.Resize.newBuilder()
                                .setTarget(createActionEntity(projectedEntity.getOid(),
                                        projectedEntity.getEntityType(), projectedEntity.getEnvironmentType()))
                                .setOldCapacity(approximateLimit)
                                .setNewCapacity(0)
                                .setCommodityType(topologyCommodityType)
                                .setCommodityAttribute(CommodityAttribute.LIMIT);
                        setHotAddRemove(resizeBuilder, resizeCommSold);
                        return resizeBuilder.build();
                    }
                    break;
                }
            }
        }
        return null;
    }

    private boolean validResizeTrigger(final double oldCapacity, final double newCapacity, @Nonnull final ResizeTO resizeTO) {
        if (!resizeTO.getResizeTriggerTraderList().isEmpty()) {
            // Scale Up: Show relevant vSan resizes when hosts are provisioned due to
            // the commodity type being scaled.
            if (newCapacity > oldCapacity) {
                Optional<ResizeTriggerTraderTO> resizeTriggerTraderTO = resizeTO.getResizeTriggerTraderList().stream()
                        .filter(resizeTriggerTrader -> resizeTriggerTrader.getRelatedCommoditiesList()
                                .contains(resizeTO.getSpecification().getBaseType())).findFirst();
                return resizeTriggerTraderTO.isPresent();
                // Scale Down: Pick related vSan host being suspended that has no reason commodities
                // since it is suspension due to low roi.
            } else if (newCapacity < oldCapacity) {
                Optional<ResizeTriggerTraderTO> resizeTriggerTraderTO = resizeTO.getResizeTriggerTraderList().stream()
                        .filter(resizeTriggerTrader -> resizeTriggerTrader.getRelatedCommoditiesList()
                                .isEmpty()).findFirst();
                return resizeTriggerTraderTO.isPresent();
            }
        }
        return true;
    }

    @Nonnull
    @Override
    public ResizeExplanation interpretExplanation(@Nonnull final ResizeTO resizeTO) {
        ResizeExplanation.Builder builder = ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(resizeTO.getStartUtilization())
                .setDeprecatedEndUtilization(resizeTO.getEndUtilization());
        if (resizeTO.hasScalingGroupId()) {
            builder.setScalingGroupId(resizeTO.getScalingGroupId());
        }
        // populate reason on ResizeExplanation
        if (resizeTO.hasReasonCommodity()) {
            final CommodityType reasonCommodityType =
                    commodityConverter.marketToTopologyCommodity(resizeTO.getReasonCommodity())
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "Resize commodity can't be converted to topology commodity format! "
                                            + resizeTO.getSpecification()));
            builder.setReason(reasonCommodityType);

        }
        return builder.build();
    }

    /**
     * Set the hot add / hot remove flag on the resize action. This is needed for the resize
     * action execution of probes like VMM.
     *
     * @param resizeBuilder Resize builder
     * @param commoditySold Commodity sold.
     */
    private void setHotAddRemove(@Nonnull ActionDTO.Resize.Builder resizeBuilder,
            @Nonnull Optional<CommoditySoldDTO> commoditySold) {
        commoditySold.filter(CommoditySoldDTO::hasHotResizeInfo)
            .map(CommoditySoldDTO::getHotResizeInfo)
            .ifPresent(hotResizeInfo -> {
                if (hotResizeInfo.hasHotAddSupported()) {
                    resizeBuilder.setHotAddSupported(hotResizeInfo.getHotAddSupported());
                }
                if (hotResizeInfo.hasHotRemoveSupported()) {
                    resizeBuilder.setHotRemoveSupported(hotResizeInfo.getHotRemoveSupported());
                }
            });
    }
}
