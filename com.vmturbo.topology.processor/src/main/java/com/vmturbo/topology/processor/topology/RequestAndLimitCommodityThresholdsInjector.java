package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl.ThresholdsImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Inject thresholds on request commodities to prevent resize up actions on request commodities.
 * We wish to receive resize down actions on request commodities but NOT resize up actions.
 * <p/>
 * Inject thresholds onto limit commodities where request is present to prevent resizing
 * down limits below requests.
 */
public class RequestAndLimitCommodityThresholdsInjector {

    private static final Set<Integer> REQUEST_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_REQUEST_VALUE,
        CommodityType.VMEM_REQUEST_VALUE
    );

    private static final Set<Integer> LIMIT_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_VALUE,
        CommodityType.VMEM_VALUE
    );

    private static final Map<Integer, Integer> REQUEST_TO_LIMIT_MAP = ImmutableMap.of(
        CommodityType.VCPU_REQUEST_VALUE, CommodityType.VCPU_VALUE,
        CommodityType.VMEM_REQUEST_VALUE, CommodityType.VMEM_VALUE
    );

    private static final Logger logger = LogManager.getLogger();

    /**
     * Inject thresholds onto Container entity Request (VCPURequest, VMemRequest)
     * commodities to prevent the generation of resize up actions for these commodities.
     * <p/>
     * Inject thresholds onto limit commodities where request is present to prevent resizing
     * down limits below requests.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in
     *              the topology and their relationships.
     * @return {@link InjectionStats} summarizing the changes made.
     */
    public InjectionStats injectThresholds(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final InjectionStats injectionStats = new InjectionStats();
        graph.entitiesOfType(EntityType.CONTAINER.getNumber()).forEach(entity ->
            injectThresholds(entity, injectionStats));

        if (injectionStats.entitiesModified > 0) {
            logger.info("Injected {} request commodity thresholds and {} limit "
                    + "commodity thresholds on {} entities.",
                injectionStats.getRequestCommoditiesModified(),
                injectionStats.getLimitCommoditiesModified(),
                injectionStats.getEntitiesModified());
        }
        return injectionStats;
    }

    /**
     * Inject thresholds onto the request commodities for a particular entity in order to prevent
     * the generation of resize up actions in the market for these commodities.
     * <p/>
     * Inject thresholds onto limit commodities where request is present to prevent resizing
     * down limits below requests.
     *
     * @param entity The entity whose request commodities on which we wish to inject thresholds.
     * @param stats for summarizing the changes made. Stats will be incremented depending
     *              to indicate the thresholds injected.
     */
    private void injectThresholds(@Nonnull final TopologyEntity entity,
                                  @Nonnull final InjectionStats stats) {
        final int initialCommoditiesModified = stats.getRequestCommoditiesModified();
        final List<CommoditySoldImpl> requests = new ArrayList<>(REQUEST_COMMODITY_TYPES.size());
        final List<CommoditySoldImpl> limits = new ArrayList<>(LIMIT_COMMODITY_TYPES.size());

        // Find the request and limit commodities to be modified
        for (CommoditySoldImpl comm : entity.getTopologyEntityImpl().getCommoditySoldListImplList()) {
            if (REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType())) {
                requests.add(comm);
            } else if (LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                && comm.getIsResizeable()) {
                // We only care about limit commodities that are resizable=true. If resizable=false, that
                // indicates the user has not set a limit on the commodity.
                limits.add(comm);
            }
        }

        // Update request and limit commodities
        for (CommoditySoldImpl requestComm : requests) {
            // Set max threshold to capacity. In the market this is translated to a capacityUpperBound
            // equal to the capacity which prevents resize up actions.
            // If there's an existing threshold, update max threshold as the min of existing max threshold
            // and request capacity.
            if (requestComm.hasThresholds()) {
                ThresholdsImpl requestCommThresholdsImpl = requestComm.getOrCreateThresholds();
                double newMaxThreshold = requestComm.getCapacity();
                if (requestCommThresholdsImpl.hasMax()) {
                    newMaxThreshold = Math.min(newMaxThreshold, requestCommThresholdsImpl.getMax());
                }
                requestCommThresholdsImpl.setMax(newMaxThreshold);
            } else {
                requestComm.setThresholds(new ThresholdsImpl().setMax(requestComm.getCapacity()));
            }
            stats.incrementRequestCommoditiesModified();

            // If there's a corresponding limit commodity for the request commodity, prevent the
            // market from resizing the limit below the request which is not permitted.
            matchingLimit(requestComm.getCommodityType().getType(), limits).ifPresent(limitComm -> {
                // If there's an existing threshold, update min threshold as the max of existing min
                // threshold and request capacity.
                if (limitComm.hasThresholds()) {
                    ThresholdsImpl limitCommThresholdsImpl = limitComm.getOrCreateThresholds();
                    double newMinThreshold = requestComm.getCapacity();
                    if (limitCommThresholdsImpl.hasMin()) {
                        newMinThreshold = Math.max(newMinThreshold, limitCommThresholdsImpl.getMin());
                    }
                    limitCommThresholdsImpl.setMin(newMinThreshold);
                } else {
                    limitComm.setThresholds(new ThresholdsImpl().setMin(requestComm.getCapacity()));
                }
                stats.incrementLimitCommoditiesModified();
            });
        }

        if (stats.getRequestCommoditiesModified() != initialCommoditiesModified) {
            stats.incrementEntitiesModified();
        }
    }

    private Optional<CommoditySoldImpl> matchingLimit(final int requestCommodityType,
                                                             @Nonnull final List<CommoditySoldImpl> limits) {
        final int limitCommodityType = REQUEST_TO_LIMIT_MAP.getOrDefault(requestCommodityType, -1);
        if (limitCommodityType >= 0) {
            return limits.stream()
                .filter(limitComm -> limitComm.getCommodityType().getType() == limitCommodityType)
                .findAny();
        } else {
            logger.error("No matching limit commodity type for request commodity type {}",
                CommodityType.forNumber(requestCommodityType));
            return Optional.empty();
        }
    }


    /**
     * Inject commodity min threshold as current usage to prevent resizing down limit or request below
     * current usage so as to avoid container running out of memory.
     * <p/>
     * Update min threshold of individual container as the max current usage from all container replicas.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in the topology and their
     *              relationships.
     * @return {@link InjectionStats} summarizing the changes made.
     */
    public InjectionStats injectMinThresholdsFromUsage(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final InjectionStats injectionStats = new InjectionStats();
        graph.entitiesOfType(EntityType.CONTAINER_SPEC.getNumber()).forEach(entity ->
            injectMinThresholdsFromUsage(entity, injectionStats));
        if (injectionStats.entitiesModified > 0) {
            logger.info("Injected {} commodity min thresholds from usage on {} entities.",
                injectionStats.getCommoditiesModified(),
                injectionStats.getEntitiesModified());
        }
        return injectionStats;
    }

    /**
     * Inject commodity min threshold as current usage to prevent resizing down limit or request below
     * current usage so as to avoid container running out of memory.
     * <p/>
     * Update min threshold of individual container as the max current usage from all container replicas
     * and the min threhsold of the parent ContainerSpec (may have been set by MovingStatisticsEditor).
     *
     * @param entity Given entity whose commodities on which we wish to inject min thresholds from usage.
     * @param stats  For summarizing the changes made. Stats will be incremented depending to indicate
     *               the thresholds injected.
     */
    private void injectMinThresholdsFromUsage(@Nonnull final TopologyEntity entity,
                                              @Nonnull final InjectionStats stats) {
        final Map<Integer, Double> commodityTypeToMaxUsageMap = new HashMap<>();

        // MovingStatisticsEditor may have already set lower bounds on ContainerSpec commodities that should
        // also be copied over to their associated Container entities.
        final Map<Integer, Double> parentSpecMinThresholdMap = entity.getTopologyEntityImpl()
            .getCommoditySoldListList()
            .stream()
            .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType()))
            .filter(comm -> comm.hasThresholds() && comm.getThresholds().hasMin())
            .collect(Collectors.toMap(comm -> comm.getCommodityType().getType(), comm -> comm.getThresholds().getMin()));

        entity.getAggregatedAndControlledEntities().forEach(container ->
            container.getTopologyEntityImpl().getCommoditySoldListList().stream()
                // Update min thresholds only for limit and request commodities.
                .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType()))
                .forEach(comm -> {
                commodityTypeToMaxUsageMap.compute(
                    comm.getCommodityType().getType(), (k, v) -> v == null ? comm.getUsed()
                        : Math.max(v, comm.getUsed()));
            }));
        entity.getAggregatedAndControlledEntities().forEach(container -> {
            container.getTopologyEntityImpl().getCommoditySoldListImplList().stream()
                .filter(comm -> LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                    || REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType()))
                .forEach(comm -> {
                    double newMinThresholds = commodityTypeToMaxUsageMap.get(comm.getCommodityType().getType());
                    Double parentThreshold = parentSpecMinThresholdMap.get(comm.getCommodityType().getType());
                    if (parentThreshold != null) {
                        newMinThresholds = Math.max(newMinThresholds, parentThreshold);
                    }

                    // If commodity has existing thresholds, update min threshold as the max of existing
                    // min threshold and max commodity usage from all container replicas.
                    if (comm.hasThresholds()) {
                        newMinThresholds = Math.max(comm.getThresholds().getMin(), newMinThresholds);
                        // If commodity max threshold is set, new min threshold cannot be larger than
                        // max threshold.
                        // For example, request commodity max threshold is request capacity. When
                        // request commodity usage is larger than capacity (which is valid), if we
                        // simply set min threshold to usage, it will cause IllegalArgumentException
                        // in the following market analysis, leading to unexpected results. So cap
                        // the min threshold as the max threshold here.
                        if (comm.getThresholds().hasMax()) {
                            newMinThresholds = Math.min(newMinThresholds, comm.getThresholds().getMax());
                        }
                        comm.getOrCreateThresholds().setMin(newMinThresholds);
                    } else {
                        comm.setThresholds(new ThresholdsImpl().setMin(newMinThresholds));
                    }
                    stats.incrementCommoditiesModified();
                });
            stats.incrementEntitiesModified();
        });
    }

    /**
     * Inject commodity min threshold as current reservation to prevent resizing down limit or request below
     * current reservation so as to avoid resize down action failure.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in the topology and their
     *              relationships.
     * @return {@link InjectionStats} summarizing the changes made.
     */
    public InjectionStats injectMinThresholdsFromReservation(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final InjectionStats injectionStats = new InjectionStats();
        graph.entitiesOfType(EntityType.VIRTUAL_MACHINE.getNumber()).forEach(entity ->
            injectMinThresholdsFromReservation(entity, injectionStats));
        if (injectionStats.entitiesModified > 0) {
            logger.info("Injected {} commodity min thresholds from reservation on {} entities.",
                injectionStats.getCommoditiesModified(),
                injectionStats.getEntitiesModified());
        }
        return injectionStats;
    }

    /**
     * Inject commodity min threshold as current reservation to prevent resizing down limit or request below
     * current reservation so as to avoid resize down action failure.
     *
     * @param entity Given entity whose commodities on which we wish to inject min thresholds from reservation.
     * @param stats  For summarizing the changes made. Stats will be incremented depending to indicate
     *               the thresholds injected.
     */
    private void injectMinThresholdsFromReservation(@Nonnull final TopologyEntity entity,
                                                    @Nonnull final InjectionStats stats) {
        // Extract reserved CPU capacity from commodity bought.
        final Optional<Double> reservedCapacityOptional = entity.getTopologyEntityImpl()
            .getCommoditiesBoughtFromProvidersList().stream()
            .filter(commBoughtGrouping -> commBoughtGrouping.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .flatMap(commBoughtGrouping -> commBoughtGrouping.getCommodityBoughtList().stream())
            .filter(commBought -> commBought.getCommodityType().getType() == CommodityType.CPU_VALUE)
            .filter(CommodityBoughtView::hasReservedCapacity)
            .map(CommodityBoughtView::getReservedCapacity)
            .findFirst();

        if (reservedCapacityOptional.isPresent()) {
            final double reservedCapacity = reservedCapacityOptional.get();
            entity.getTopologyEntityImpl()
                .getCommoditySoldListImplList().stream()
                .filter(commSold -> commSold.getCommodityType().getType() == CommodityType.VCPU_VALUE)
                .forEach(commSold -> {
                    // If commodity has existing thresholds, update min threshold.
                    if (commSold.hasThresholds()) {
                        // Set minThreshold to Math.min(Math.max(minThreshold, reservedCapacity), maxThreshold))
                        final ThresholdsImpl threshold = commSold.getOrCreateThresholds();
                        double newMinThreshold = Math.max(threshold.getMin(), reservedCapacity);
                        if (threshold.hasMax()) {
                            newMinThreshold = Math.min(newMinThreshold, threshold.getMax());
                        }
                        threshold.setMin(newMinThreshold);
                    } else {
                        commSold.setThresholds(new ThresholdsImpl().setMin(reservedCapacity));
                    }
                    stats.incrementCommoditiesModified();
                    stats.incrementEntitiesModified();
                });
        }
    }

    /**
     * Statistics about the number of entities and commodities on which we inject thresholds.
     */
    public static class InjectionStats {
        private int entitiesModified;
        private int requestCommoditiesModified;
        private int limitCommoditiesModified;
        private int commoditiesModified;

        /**
         * Create a new {@link InjectionStats}.
         */
        public InjectionStats() {

        }

        private void incrementEntitiesModified() {
            entitiesModified++;
        }

        private void incrementRequestCommoditiesModified() {
            requestCommoditiesModified++;
        }

        private void incrementLimitCommoditiesModified() {
            limitCommoditiesModified++;
        }

        private void incrementCommoditiesModified() {
            commoditiesModified++;
        }

        /**
         * Get the number of entities modified.
         *
         * @return The number of entities modified.
         */
        public int getEntitiesModified() {
            return entitiesModified;
        }

        /**
         * Get the number of request commodities modified.
         *
         * @return The number of request commodities modified.
         */
        public int getRequestCommoditiesModified() {
            return requestCommoditiesModified;
        }

        /**
         * Get the number of limit commodities modified.
         *
         * @return The number of limit commodities modified.
         */
        public int getLimitCommoditiesModified() {
            return limitCommoditiesModified;
        }

        /**
         * Get the number of commodities modified.
         *
         * @return The number of commodities modified.
         */
        public int getCommoditiesModified() {
            return commoditiesModified;
        }
    }
}
