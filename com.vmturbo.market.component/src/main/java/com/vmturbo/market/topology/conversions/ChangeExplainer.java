package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityLookupType;
import com.vmturbo.market.topology.conversions.CommoditiesResizeTracker.CommodityTypeWithLookup;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The abstract change explainer. It is used to explain actions which needed resizing before analysis - like Cloud VMS,
 * VDI entities, RDS entities etc. To explain regular actions, use the {@link DefaultChangeExplainer} class.
 * For merged actions like in the case of RDS, we use {@link MergedActionChangeExplainer}
 */
public abstract class ChangeExplainer {

    private static final Logger logger = LogManager.getLogger();

    private final CommoditiesResizeTracker commoditiesResizeTracker;
    protected final CloudTopologyConverter cloudTc;
    private final CommodityIndex commodityIndex;

    /**
     * The public constructor.
     *
     * @param commoditiesResizeTracker The resize tracker
     * @param cloudTc cloud topology converter
     * @param commodityIndex commodity index
     */
    public ChangeExplainer(CommoditiesResizeTracker commoditiesResizeTracker,
                           CloudTopologyConverter cloudTc,
                           CommodityIndex commodityIndex) {
        this.commoditiesResizeTracker = commoditiesResizeTracker;
        this.cloudTc = cloudTc;
        this.commodityIndex = commodityIndex;
    }

    /**
     * Abstract method to create the change explanations. The implementations are in the subclasses.
     * @param moveTO the move to explain
     * @param savings the savings
     * @param projectedTopology the rojected topology
     * @return A change provider explanation
     */
    public abstract Optional<ChangeProviderExplanation.Builder> changeExplanationFromTracker(
            @Nonnull MoveTO moveTO,
            @Nonnull CalculatedSavings savings,
            @Nonnull Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopology);

    protected Optional<ChangeProviderExplanation.Builder> getCongestedExplanationFromTracker(long actionTargetId, long sellerId) {
        Set<CommodityTypeWithLookup> congestedCommodityTypes =
                commoditiesResizeTracker.getCongestedCommodityTypes(actionTargetId, sellerId);
        if (congestedCommodityTypes != null && !congestedCommodityTypes.isEmpty()) {
            Congestion.Builder congestionBuilder = Congestion.newBuilder().addAllCongestedCommodities(
                    commTypes2ReasonCommodities(congestedCommodityTypes.stream().map(
                            CommodityTypeWithLookup::commodityType).collect(Collectors.toSet())));
            logger.debug("CongestedCommodities from tracker for buyer: {}, seller: {} : [{}]",
                    actionTargetId, sellerId,
                    congestedCommodityTypes.stream().map(type -> type.toString()).collect(Collectors.joining()));
            return Optional.of(ChangeProviderExplanation.newBuilder().setCongestion(
                    congestionBuilder));
        }
        return Optional.empty();
    }

    /**
     * Commodities with either lower projected capacity values or higher projected values compared to original topology.
     *
     * @param moveTO the moveTO from m2
     * @param actionTargetId   current action.
     * @param projectedTopology    projected topology indexed by id.
     * @param sellerId the seller which supplied the shopping list before anlaysis took place
     * @return Pair set of lower projected commodities and higher projected commodity.
     */
    @Nonnull
    protected Pair<Set<CommodityType>, Set<CommodityType>> calculateCommWithChangingProjectedCapacity(
            final MoveTO moveTO,
            final long actionTargetId,
            @Nonnull final Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopology,
            long sellerId) {
        Set<CommodityTypeWithLookup> underutilizedCommTypesWithLookup = commoditiesResizeTracker
                .getUnderutilizedCommodityTypes(actionTargetId, sellerId);
        final Set<CommodityType> lowerProjectedCapacity = Sets.newHashSet();
        final Set<CommodityType> higherProjectedCapacity = Sets.newHashSet();
        if (underutilizedCommTypesWithLookup.isEmpty()) {
            return new Pair<>(lowerProjectedCapacity, higherProjectedCapacity);
        }
        final Map<CommodityType, TopologyDTO.CommoditySoldDTO> originalCommoditySoldBySource = Maps.newHashMap();
        final Map<CommodityType, TopologyDTO.CommoditySoldDTO> originalCommoditySoldByTarget = Maps.newHashMap();
        Map<CommodityType, TopologyDTO.CommoditySoldDTO> projectedCommoditySoldByDestination = Maps.newHashMap();
        Map<CommodityType, TopologyDTO.CommoditySoldDTO> projectedCommoditySoldByTarget = Maps.newHashMap();
        // Segregate under-utilized commodities by lookup types - consumer lookup and provider lookup.
        // Some commodities like VMEM/VCPU are sold by the consumer. The projected capacity needs
        // to be compared with original capacity at the consumer.
        // Some commodities like IOThroughput/NetThroughput are bought by the consumer, and sold
        // by the provider. The projected capacity needs to be compared with original capacity
        // at the provider for these commodities.
        Map<CommodityLookupType, List<CommodityTypeWithLookup>> underUtilizedCommoditiesByLookup =
                underutilizedCommTypesWithLookup.stream().collect(Collectors.groupingBy(CommodityTypeWithLookup::lookupType));
        List<CommodityType> underUtilCommsWithConsumerLookup = underUtilizedCommoditiesByLookup.containsKey(CommodityLookupType.CONSUMER)
                ? underUtilizedCommoditiesByLookup.get(CommodityLookupType.CONSUMER).stream()
                .map(CommodityTypeWithLookup::commodityType).collect(Collectors.toList())
                : Collections.emptyList();
        List<CommodityType> underUtilizedCommsWithProviderLookup = underUtilizedCommoditiesByLookup.containsKey(CommodityLookupType.PROVIDER)
                ? underUtilizedCommoditiesByLookup.get(CommodityLookupType.PROVIDER).stream()
                .map(CommodityTypeWithLookup::commodityType).collect(Collectors.toList())
                : Collections.emptyList();

        // populate projectedCommoditySoldByTarget and originalCommoditySoldByTarget
        if (!underUtilCommsWithConsumerLookup.isEmpty()) {
            projectedCommoditySoldByTarget = projectedTopology.containsKey(actionTargetId)
                    ? projectedTopology.get(actionTargetId).getEntity()
                    .getCommoditySoldListList().stream()
                    .filter(commoditySoldDTO -> underUtilCommsWithConsumerLookup.contains(commoditySoldDTO.getCommodityType()))
                    .collect(Collectors.toMap(TopologyDTO.CommoditySoldDTO::getCommodityType, Function.identity()))
                    : Collections.emptyMap();
            underUtilCommsWithConsumerLookup.forEach(comm -> commodityIndex.getCommSold(actionTargetId, comm)
                    .ifPresent(cSold -> originalCommoditySoldByTarget.put(comm, cSold)));
        }

        // populate projectedCommoditySoldByDestination and originalCommoditySoldBySource
        if (!underUtilizedCommsWithProviderLookup.isEmpty()) {
            long sourceId = moveTO.getSource();
            if (cloudTc.isMarketTier(sourceId)) {
                Optional<Long> sourceTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, actionTargetId, true);
                if (sourceTierId.isPresent()) {
                    sourceId = sourceTierId.get();
                    populateCommoditiesSoldByCommodityTypeFromCommodityIndex(sourceId, underUtilizedCommsWithProviderLookup, originalCommoditySoldBySource);
                }
            } else {
                populateCommoditiesSoldByCommodityTypeFromCommodityIndex(sourceId, underUtilizedCommsWithProviderLookup, originalCommoditySoldBySource);
            }

            long destinationId = moveTO.getDestination();
            if (cloudTc.isMarketTier(destinationId)) {
                Optional<Long> destinationTierId = cloudTc.getSourceOrDestinationTierFromMoveTo(moveTO, actionTargetId, false);
                if (destinationTierId.isPresent()) {
                    destinationId = destinationTierId.get();
                    populateCommoditiesSoldByCommodityTypeFromCommodityIndex(destinationId, underUtilizedCommsWithProviderLookup, projectedCommoditySoldByDestination);
                }
            } else {
                if (projectedTopology.get(destinationId) != null) {
                    projectedCommoditySoldByDestination = projectedTopology.get(destinationId).getEntity()
                                    .getCommoditySoldListList().stream()
                                    .filter(commoditySoldDTO -> underUtilizedCommsWithProviderLookup.contains(commoditySoldDTO.getCommodityType()))
                                    .collect(Collectors.toMap(TopologyDTO.CommoditySoldDTO::getCommodityType, Function.identity()));
                }
            }
        }
        return getCommoditiesWithLowerAndHigherProjectedCapacity(underutilizedCommTypesWithLookup, originalCommoditySoldBySource,
                originalCommoditySoldByTarget, projectedCommoditySoldByDestination, projectedCommoditySoldByTarget);
    }

    private Pair<Set<CommodityType>, Set<CommodityType>> getCommoditiesWithLowerAndHigherProjectedCapacity(
            final Set<CommodityTypeWithLookup> underutilizedCommTypesWithLookup,
            final Map<CommodityType, TopologyDTO.CommoditySoldDTO> originalCommoditySoldBySource,
            final Map<CommodityType, TopologyDTO.CommoditySoldDTO> originalCommoditySoldByTarget,
            final Map<CommodityType, TopologyDTO.CommoditySoldDTO> projectedCommoditySoldByDestination,
            final Map<CommodityType, TopologyDTO.CommoditySoldDTO> projectedCommoditySoldByTarget) {
        final Set<CommodityType> lowerProjectedCapacity = Sets.newHashSet();
        final Set<CommodityType> higherProjectedCapacity = Sets.newHashSet();
        // Loop through the under utilized commodities, and compare the capacity of the commodity
        // sold of the original with projected.
        for (CommodityTypeWithLookup underutilizedComm : underutilizedCommTypesWithLookup) {
            switch (underutilizedComm.lookupType()) {
                case CONSUMER:
                    if (projectedCommoditySoldByTarget.containsKey(underutilizedComm.commodityType())
                            && originalCommoditySoldByTarget.containsKey(underutilizedComm.commodityType())) {
                        double projectedCapacity = projectedCommoditySoldByTarget.get(underutilizedComm.commodityType()).getCapacity();
                        double originalCapacity = originalCommoditySoldByTarget.get(underutilizedComm.commodityType()).getCapacity();
                        if (projectedCapacity < originalCapacity) {
                            lowerProjectedCapacity.add(underutilizedComm.commodityType());
                        } else if (projectedCapacity > originalCapacity) {
                            higherProjectedCapacity.add(underutilizedComm.commodityType());
                        }
                    }
                    break;
                case PROVIDER:
                    if (projectedCommoditySoldByDestination.containsKey(underutilizedComm.commodityType())
                            && originalCommoditySoldBySource.containsKey(underutilizedComm.commodityType())) {
                        double projectedCapacity = projectedCommoditySoldByDestination.get(underutilizedComm.commodityType()).getCapacity();
                        double originalCapacity = originalCommoditySoldBySource.get(underutilizedComm.commodityType()).getCapacity();
                        if (projectedCapacity < originalCapacity) {
                            lowerProjectedCapacity.add(underutilizedComm.commodityType());
                        } else if (projectedCapacity > originalCapacity) {
                            higherProjectedCapacity.add(underutilizedComm.commodityType());
                        }
                    }
                    break;
            }
        }
        return new Pair<>(lowerProjectedCapacity, higherProjectedCapacity);
    }

    /**
     * Convert collection of {@link CommodityType}s to collection of {@link ActionDTO.Explanation.ReasonCommodity}.
     * @param types the commodity types to convert
     * @return collection of reason commodities
     */
    public static Collection<ActionDTO.Explanation.ReasonCommodity> commTypes2ReasonCommodities(
            Collection<CommodityType> types) {
        return types.stream().map(commType2ReasonCommodity()).collect(Collectors.toList());
    }

    private static Function<CommodityType, ActionDTO.Explanation.ReasonCommodity> commType2ReasonCommodity() {
        return ct -> ActionDTO.Explanation.ReasonCommodity.newBuilder().setCommodityType(ct).build();
    }

    private void populateCommoditiesSoldByCommodityTypeFromCommodityIndex(long entityId, List<CommodityType> commodityTypeSoldFilter,
                                                                          Map<CommodityType, TopologyDTO.CommoditySoldDTO> mapToPopulate) {
        for (CommodityType commType : commodityTypeSoldFilter) {
            commodityIndex.getCommSold(entityId, commType)
                    .ifPresent(cSold -> mapToPopulate.put(commType, cSold));
        }
    }
}
