package com.vmturbo.topology.processor.history.percentile;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Calculate and provide percentile historical value for topology commodities.
 */
public class PercentileEditor extends
                AbstractCachingHistoricalEditor<PercentileCommodityData,
                    PercentilePersistenceTask,
                    PercentileHistoricalEditorConfig,
                    PercentileRecord,
                    StatsHistoryServiceStub> {
    private static final Logger logger = LogManager.getLogger();
    // certain sold commodities should have percentile calculated from real-time points
    // even if dedicated percentile utilizations are absent in the mediation
    private static final Set<CommodityType> REQUIRED_SOLD_COMMODITY_TYPES = ImmutableSet
                    .of(CommodityDTO.CommodityType.VCPU,
                        CommodityDTO.CommodityType.VMEM);
    // percentile on a bought commodity will not add up unless there is no more than one
    // consumer per provider, so only certain commodity types are applicable
    private static final Set<CommodityType> ENABLED_BOUGHT_COMMODITY_TYPES = ImmutableSet
                    .of(CommodityDTO.CommodityType.IMAGE_CPU,
                        CommodityDTO.CommodityType.IMAGE_MEM,
                        CommodityDTO.CommodityType.IMAGE_STORAGE);
    private static final DataMetricSummary SETTINGS_CHANGE_SUMMARY_METRIC =
                DataMetricSummary.builder()
                                .withName("tp_historical_percentile_window_change")
                                .withHelp("The time spent on handling changes of observation windows settings "
                                          + "(this is also part of tp_historical_initialization_time_percentile)")
                                .build();
    private static final DataMetricSummary MAINTENANCE_SUMMARY_METRIC =
                DataMetricSummary.builder()
                               .withName("tp_historical_percentile_maintenance")
                               .withHelp("The time spent on daily maintenance of percentile cache "
                                         + "(this is also part of tp_historical_completion_time_percentile)")
                               .build();

    private boolean historyInitialized;
    private long lastCheckpointMs;
    private long checkpoints;
    private TopologyGraph<TopologyEntity> graph;
    /**
     * Maintain per-entity observation periods for all entities ever seen
     * since the component startup, to handle setting changes individually.
     * Use oids to not hold onto entities.
     * Note that maintaining this imposes considerable performance cost
     * as it requires iterations over topology for every broadcast and extra
     * loading times when some periods change.
     * We should consider keeping per-entity-type period settings only.
     * Or preferably even just one global observation window setting.
     */
    private Cache<Long, Integer> observationPeriods = CacheBuilder.newBuilder()
                    .expireAfterWrite(getMaxObservationWindow() + 1, TimeUnit.DAYS)
                    .build();

    /**
     * Construct the instance of percentile editor.
     *
     * @param config configuration values
     * @param statsHistoryClient persistence component access handler
     */
    public PercentileEditor(PercentileHistoricalEditorConfig config,
                            StatsHistoryServiceStub statsHistoryClient) {
        super(config, statsHistoryClient, PercentilePersistenceTask::new, PercentileCommodityData::new);
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        return true;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        return true;
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        return commSold.hasUtilizationData() || REQUIRED_SOLD_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commSold.getCommodityType().getType()));
    }

    @Override
    public boolean
           isCommodityApplicable(TopologyEntity entity,
                                 TopologyDTO.CommodityBoughtDTO.Builder commBought) {
        return commBought.hasUtilizationData() && ENABLED_BOUGHT_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commBought.getCommodityType().getType()));
    }

    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    public void initContext(@Nonnull GraphWithSettings graph,
                            @Nonnull ICommodityFieldAccessor accessor)
                    throws HistoryCalculationException {
        super.initContext(graph, accessor);
        this.graph = graph.getTopologyGraph();

        loadPersistedData();
        checkObservationPeriodsChanged(graph);
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull List<EntityCommodityReference> commodityRefs) {
        // percentile data will be loaded in a single-threaded way into blobs (not chunked)
        // initialize cache values for entries from topology
        // in order to get the percentiles for them calculated as well if they do not get read
        commodityRefs.forEach(commRef -> {
            EntityCommodityFieldReference field =
                            new EntityCommodityFieldReference(commRef,
                                                              CommodityField.USED);
            getCache().computeIfAbsent(field, fieldRef -> {
                PercentileCommodityData data = historyDataCreator.get();
                data.init(field, null, getConfig(), getCommodityFieldAccessor());
                return data;
            });
        });
        return Collections.emptyList();
    }

    @Override
    public void completeBroadcast() throws HistoryCalculationException {
        // persist the daily blob
        PercentileCounts.Builder builder = PercentileCounts.newBuilder();
        getCache().forEach((commRef, data) -> {
            builder.addPercentileRecords(data.getUtilizationCountStore().getLatestCountsRecord());
        });
        new PercentilePersistenceTask(getStatsHistoryClient(), lastCheckpointMs).save(builder.build());

        // perform daily maintenance if needed - synchronously within broadcast (consider scheduling)
        maintenance();
    }

    private void loadPersistedData() throws HistoryCalculationException {
        if (!historyInitialized) {
            // read the latest and full window blobs if haven't yet, set into cache
            long checkpointMs = getCheckpoint();
            logger.debug("Initializing percentile latest window data for timestamp {}",
                         () -> checkpointMs);
            Map<EntityCommodityFieldReference, PercentileRecord> latestPage =
                        new PercentilePersistenceTask(getStatsHistoryClient(),
                                                      checkpointMs).load(Collections.emptyList(), getConfig());
            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> latestEntry : latestPage.entrySet()) {
                PercentileCommodityData data =
                                getCache().computeIfAbsent(latestEntry.getKey(),
                                                       ref -> historyDataCreator.get());
                data.init(latestEntry.getKey(), latestEntry.getValue(), getConfig(),
                          getCommodityFieldAccessor());
            }

            logger.debug("Initializing percentile full window data");
            Map<EntityCommodityFieldReference, PercentileRecord> fullPage =
                            new PercentilePersistenceTask(getStatsHistoryClient(),
                                                          0).load(Collections.emptyList(), getConfig());
            for (Map.Entry<EntityCommodityFieldReference, PercentileRecord> fullEntry : fullPage.entrySet()) {
                PercentileCommodityData data =
                                getCache().computeIfAbsent(fullEntry.getKey(),
                                                      ref -> historyDataCreator.get());
                data.getUtilizationCountStore().addFullCountsRecord(fullEntry.getValue(), true);
            }
            historyInitialized = true;
        }
    }

    private void checkObservationPeriodsChanged(@Nonnull GraphWithSettings graph) {
        Map<Long, Integer> entity2period = graph.getTopologyGraph()
                        .getEntities(Collections.emptySet())
                        .collect(Collectors.toMap(TopologyEntity::getOid,
                                                  entity -> getConfig()
                                                      .getObservationPeriod(entity.getOid())));
        Map<Long, Integer> previousEntity2period = observationPeriods
                        .getAllPresent(entity2period.keySet());
        if (observationPeriods.size() > 0
            && !previousEntity2period.equals(entity2period)) {
            try (DataMetricTimer timer = SETTINGS_CHANGE_SUMMARY_METRIC.startTimer()) {
                Stopwatch sw = Stopwatch.createStarted();
                Map<Long, ValueDifference<Integer>> changedPeriods = Maps
                                .difference(previousEntity2period, entity2period).entriesDiffering();
                // some observation window settings changed, some cache data becomes invalid
                int maxOfChangedPeriods = changedPeriods.values().stream()
                                .map(ValueDifference::rightValue).max(Comparator.comparingInt(i -> i)).orElse(0);
                // TODO dmitry read as many page blobs from persistence as constitute the max of new periods
                // and accumulate them into percentile cache, respect per-entity observation window settings
                logger.warn("Percentile observation windows changed for {} entities, recalculated from {} pages in {}",
                            changedPeriods.size(), maxOfChangedPeriods, sw);
            }
        }
        observationPeriods.putAll(entity2period);
    }

    private void maintenance() throws HistoryCalculationException {
        long checkpointMs = getCheckpoint();
        if (checkpointMs <= lastCheckpointMs) {
            logger.trace("Percentile cache checkpoint skipped - not enough time passed since last checkpoint "
                         + lastCheckpointMs);
            return;
        }
        try (DataMetricTimer timer = MAINTENANCE_SUMMARY_METRIC.startTimer()) {
            logger.debug("Performing percentile cache maintenance {}", ++checkpoints);
            Stopwatch sw = Stopwatch.createStarted();
            // TODO dmitry load the page(s) that go out of all possible observation windows
            // there can be more than 1 page becoming obsolete for each window value,
            // depending on time difference between checkpointMs and lastCheckpointMs
            // i.e. we need to load 1 to 4 pages
            // worst case if broadcasts are delayed and all windows are configured, perhaps 8 pages
            Set<Integer> periods = graph.getEntities(Collections.emptySet())
                            .map(entity -> getConfig().getObservationPeriod(entity.getOid()))
                            .collect(Collectors.toSet());

            lastCheckpointMs = checkpointMs;
            // TODO dmitry checkpoint all entries in the cache, persist the full window
            // checkpoint per-entity as configured windows can be different
            // getUtilizationCountStore().checkpoint();
            // PercentilePersistenceTask.save()
            logger.warn("Percentile cache maintenance {} took {}", checkpoints, sw);
        }
    }

    private long getCheckpoint() {
        long checkpointMs = System.currentTimeMillis();
        double window = getConfig().getMaintenanceWindowHours() * Units.HOUR_MS;
        return (long)(Math.floor(checkpointMs / window) * window);
    }

    private static int getMaxObservationWindow() {
        return (int)EntitySettingSpecs.PercentileObservationPeriodVirtualMachine.getSettingSpec()
                        .getNumericSettingValueType().getMax();
    }
}
