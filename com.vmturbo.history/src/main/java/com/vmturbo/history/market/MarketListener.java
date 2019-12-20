package com.vmturbo.history.market;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.history.stats.priceindex.TopologyPriceIndexVisitor;
import com.vmturbo.history.stats.priceindex.TopologyPriceIndices;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Receive and process both projected topologyies and new price index values calculated by
 * Market analysis.
 *
 * Note that this applies to both Live Market topologies and Plan Market topologies.
 *
 * The processing sequence is:
 * <ol>
 *     <li>TopologyProcessor:  topology ->  Market
 *     <li>Market: projectedTopology -> ProjectedTopologyListener.
 *     <li>Market: priceIndexMessage -> PriceIndexListener.
 * </ol>
 * There is no assurance of ordering between the ProjectedTopology and PriceIndex messages.
 */
public class MarketListener implements ProjectedTopologyListener {

    private static Logger logger = LogManager.getLogger(MarketListener.class);

    private final PlanStatsWriter planStatsWriter;
    private final DBPriceIndexVisitorFactory visitorFactory;
    private final long realtimeTopologyContextId;
    private final StatsAvailabilityTracker availabilityTracker;
    private final ProjectedStatsStore projectedStatsStore;

    /** Entity types appearing in topology for which we cannot save price index data. */
    private Multimap<String, String> unsavedEntityTypes = HashMultimap.create();
    /** Used to ensure entity types with no commodities appear in above multimap. **/
    private static final String UNSAVED_ENTITY_DUMMY_COMMODITY = "-";

    /**
     * Constructs a listener class for the Projected Topologies and Price Index information
     * produced by the Market.
     *
     * @param planStatsWriter the DB access class for the stats from the plan topology
     * @param realtimeTopologyContextId the context ID of a realtime topology.
     * @param statsAvailabilityTracker sends notifications when stats are available.
     */
    public MarketListener(@Nonnull final PlanStatsWriter planStatsWriter,
                          @Nonnull final DBPriceIndexVisitorFactory visitorFactory,
                          final long realtimeTopologyContextId,
                          @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker,
                          @Nonnull final ProjectedStatsStore projectedStatsStore) {
        this.planStatsWriter = Objects.requireNonNull(planStatsWriter);
        this.visitorFactory = Objects.requireNonNull(visitorFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                        @Nonnull final TopologyInfo sourceTopologyInfo,
                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> topologyDTOs) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        final String contextType = topologyContextId == realtimeTopologyContextId ?
            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL : SharedMetrics.PLAN_CONTEXT_TYPE_LABEL;

        try (final DataMetricTimer timer = SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY
                .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, contextType)
                .startTimer()) {
            if (topologyContextId != realtimeTopologyContextId) {
                handlePlanProjectedTopology(projectedTopologyId, sourceTopologyInfo, topologyDTOs);
            } else {
                handleLiveProjectedTopology(projectedTopologyId, sourceTopologyInfo, topologyDTOs);
            }
        } catch (Exception e) {
            logger.error("An error happened while constructing a listener class for " +
                    "the Projected Topologies and Price Index information produced by the " +
                    "Market", e);
        }
    }

    private void handlePlanProjectedTopology(final long projectedTopologyId,
                                             @Nonnull final TopologyInfo sourceTopologyInfo,
                                             @Nonnull final RemoteIterator<ProjectedTopologyEntity>
                                                     dtosIterator) throws Exception {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        try {
            logger.info("Receiving projected plan topology, context: {}, projected id: {}, "
                            + "source id: {}, " + "source topology creation time: {}",
                    topologyContextId, projectedTopologyId,
                    sourceTopologyInfo.getTopologyId(), sourceTopologyInfo.getCreationTime());
            int numEntities = planStatsWriter.processProjectedChunks(sourceTopologyInfo, dtosIterator);
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                    .observe((double) numEntities);
            availabilityTracker.projectedTopologyAvailable(topologyContextId,
                    TopologyContextType.PLAN, true);
        } catch (Exception e) {
            logger.warn("Error occurred while processing data for projected topology "
                    + "broadcast " + sourceTopologyInfo.getTopologyId(), e);
            availabilityTracker.projectedTopologyAvailable(topologyContextId,
                    TopologyContextType.PLAN, false);
            throw new RuntimeException("Error occurred while receiving topology broadcast", e);
        }
    }

    private void handleLiveProjectedTopology(final long projectedTopologyId,
                                             @Nonnull final TopologyInfo sourceTopologyInfo,
                                             @Nonnull final RemoteIterator<ProjectedTopologyEntity> dtosIterator)
            throws CommunicationException, InterruptedException {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        try {
            logger.info("Receiving projected live topology, context: {}, projected id: {}, source id: {}",
                    topologyContextId, projectedTopologyId, sourceTopologyInfo.getTopologyId());
            final TopologyPriceIndices.Builder indicesBuilder =
                    TopologyPriceIndices.builder(sourceTopologyInfo);

            final RemoteIterator<ProjectedTopologyEntity> priceIndexRecordingIterator =
                    new RemoteIterator<ProjectedTopologyEntity>() {
                        @Override
                        public boolean hasNext() {
                            return dtosIterator.hasNext();
                        }

                        @Nonnull
                        @Override
                        public Collection<ProjectedTopologyEntity> nextChunk() throws InterruptedException, TimeoutException, CommunicationException {
                            final Collection<ProjectedTopologyEntity> nextChunk = dtosIterator.nextChunk();
                            for (ProjectedTopologyEntity entity : nextChunk) {
                                checkForUnknownDBEntity(entity);
                                indicesBuilder.addEntity(entity);
                            }
                            return nextChunk;
                        }
                    };

            final long numEntities = projectedStatsStore.updateProjectedTopology(priceIndexRecordingIterator);
            logUnsavedEntityTypes();
            logger.info("{} entities updated", numEntities);

            // This needs to happen after updating the projected topology.
            // The projected stats store consumes the iterator, which should fill up the price
            // indices.
            final TopologyPriceIndices priceIndices = indicesBuilder.build();
            priceIndices.visit(new TopologyPriceIndexVisitor() {
                Set<Integer> entityTypes = new HashSet<>();

                @Override
                public void visit(final Integer entityType, final EnvironmentType environmentType, final Map<Long, Double> priceIdxByEntityId) throws VmtDbException {
                    entityTypes.add(entityType);
                }

                @Override
                public void onComplete() throws VmtDbException {
                    logger.info("PRICE_INDEX ENTITY TYPES: {}", entityTypes);
                }
            });
            priceIndices.visit(visitorFactory.newVisitor(sourceTopologyInfo));


            availabilityTracker.projectedTopologyAvailable(topologyContextId,
                    TopologyContextType.LIVE, true);
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .observe((double) numEntities);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while processing projected live topology " +
                    projectedTopologyId, e);
            availabilityTracker.projectedTopologyAvailable(topologyContextId,
                    TopologyContextType.LIVE, false);
        } catch (Exception e) {
            logger.warn("Error occurred while processing data for projected live topology "
                    + "broadcast " + projectedTopologyId, e);
            availabilityTracker.projectedTopologyAvailable(topologyContextId,
                    TopologyContextType.LIVE, false);
        }
    }


    /**
     * Check each entity received in the topology to see whether entity stats tables exist for the
     * entity type.
     *
     * <p>We record unsaved entity types along with a list of all bought or sold commodity types
     * appearing for each (combined list across the whole topology). We report these details at
     * the end of the topology.</p>
     *
     * <p>Entity types appearing in {@link HistoryStatsUtils#SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES}
     * not reported.</p>
     *
     * @param entity an entity from the topology
     */
    private void checkForUnknownDBEntity(final ProjectedTopologyEntity entity) {
        // SDK entity type number
        final int typeNum = entity.getEntity().getEntityType();
        if (!HistoryStatsUtils.SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES.contains(typeNum)) {
            // SDK entity type enum
            final EntityType type = EntityType.forNumber(typeNum);
            // DB entity type
            final com.vmturbo.history.db.EntityType dbType
                    = HistoryStatsUtils.SDK_ENTITY_TYPE_TO_ENTITY_TYPE.get(type);
            final String typeName = type != null ? type.name() : ("#" + typeNum);

            if (type == null || dbType == null) {
                // dummy entry to ensure the entity type gets recorded, even if it comes with no
                // commodities
                unsavedEntityTypes.put(typeName, UNSAVED_ENTITY_DUMMY_COMMODITY);
                // record commodity type for all bought/sold commodities
                entity.getEntity().getCommoditySoldListList().forEach(
                        c -> recordUnsavedEntityCommodity(typeName, c.getCommodityType()));
                entity.getEntity().getCommoditiesBoughtFromProvidersList().forEach(
                        cbp -> cbp.getCommodityBoughtList().forEach(
                                c -> recordUnsavedEntityCommodity(typeName, c.getCommodityType())));
            }
        }
    }

    /**
     * Record an unsaved entity type with one of its associated commodities.
     *
     * @param typeName      name of entity type
     * @param commodityType commodity type to record
     */
    private void recordUnsavedEntityCommodity(final String typeName,
            final TopologyDTO.CommodityType commodityType) {
        unsavedEntityTypes.put(typeName,
                CommodityType.forNumber(commodityType.getType()).name());
    }

    /**
     * Log unsaved entity types that were encountered in the topology.
     */
    private void logUnsavedEntityTypes() {
        for (String entityType : unsavedEntityTypes.keySet()) {
            final Collection<String> commodities = unsavedEntityTypes.get(entityType).stream()
                    // don't show dummy commodity entries
                    .filter(c -> !c.equals(UNSAVED_ENTITY_DUMMY_COMMODITY))
                    .sorted()
                    .collect(Collectors.toList());
            logger.warn("Cannot save SDK entity type {}, appearing with commodity types {}",
                    entityType, commodities);
        }
    }
}
