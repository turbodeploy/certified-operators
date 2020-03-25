package com.vmturbo.topology.processor.history.timeslot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractBackgroundLoadingHistoricalEditor;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;

/**
 * Calculate and provide time slot historical values for topology commodities.
 */
public class TimeSlotEditor extends
    AbstractBackgroundLoadingHistoricalEditor<TimeSlotCommodityData,
                TimeSlotLoadingTask,
                TimeslotHistoricalEditorConfig,
                List<Pair<Long, StatRecord>>,
                StatsHistoryServiceBlockingStub,
                Void> {
    private static final Set<CommodityType> ENABLED_BOUGHT_COMMODITY_TYPES = ImmutableSet
                    .of(CommodityDTO.CommodityType.POOL_CPU,
                        CommodityDTO.CommodityType.POOL_MEM,
                        CommodityDTO.CommodityType.POOL_STORAGE);

    private final Cache<Long, Map<Long, Integer>> topologyToApplicableEntityToPeriod =
                    CacheBuilder.newBuilder().expireAfterAccess(12, TimeUnit.HOURS).build();

    /**
     * Stores information about time when we first time started TP. It is required to re-enable back
     * TimeSlot analysis in the following scenario:
     * <ol>
     *     <li> TP started, but background loading process failed and whole TimeSlot analysis
     *     was disabled.
     *     <li> In case TP successfully was working and collecting data for more than observation
     *     period time required for TimeSlot analysis we want to enable TimeSlot analysis back.
     *     Moreover since that moment of time we are clearing background loading statistic and
     *     background loading is ready to process big amount of data in background mode as usually.
     * </ol>
     */
    private long startTimestamp;

    /**
     * Construct the timeslot historical values editor.
     *
     * @param config configuration settings
     * @param statsHistoryClient history component stub
     * @param loadingPool loads data for time slot analysis in background.
     */
    public TimeSlotEditor(TimeslotHistoricalEditorConfig config,
                    StatsHistoryServiceBlockingStub statsHistoryClient,
                    @Nonnull ExecutorService loadingPool) {
        this(config, statsHistoryClient, loadingPool, TimeSlotLoadingTask::new);
    }

    /**
     * Construct the timeslot historical values editor.
     *
     * @param config configuration settings
     * @param statsHistoryClient history component stub
     * @param loadingPool loads data for time slot analysis in background.
     * @param loadingTaskSupplier creator of task that will do background loading
     */
    public TimeSlotEditor(TimeslotHistoricalEditorConfig config,
                    StatsHistoryServiceBlockingStub statsHistoryClient,
                    @Nonnull ExecutorService loadingPool,
                    @Nonnull Function<StatsHistoryServiceBlockingStub, TimeSlotLoadingTask> loadingTaskSupplier) {
        super(config, statsHistoryClient, loadingTaskSupplier, TimeSlotCommodityData::new,
                        loadingPool);
    }

    @Override
    public void initContext(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> eligibleComms)
                    throws HistoryCalculationException, InterruptedException {
        super.initContext(context, eligibleComms);
        if (!context.isPlan() && startTimestamp <= 0) {
            startTimestamp = getConfig().getClock().millis();
        }
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        return true;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        return entity.getEntityType() == EntityType.BUSINESS_USER_VALUE;
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        return false;
    }

    @Override
    public boolean
           isCommodityApplicable(TopologyEntity entity,
                                 TopologyDTO.CommodityBoughtDTO.Builder commBought) {
        return ENABLED_BOUGHT_COMMODITY_TYPES
                        .contains(CommodityType.forNumber(commBought.getCommodityType().getType()));
    }

    @Override
    public boolean isMandatory() {
        return false;
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
        if (!hasEnoughHistoricalData(context, commodityRefs)) {
            return Collections.emptyList();
        }
        List<EntityCommodityReference> uninitializedCommodities =
                        gatherUninitializedCommodities(commodityRefs);
        // partition by configured observation window first
        // we are only interested in business users
        final Map<Long, Integer> entity2period = getApplicableEntityToPeriod(context);
        Map<Integer, List<EntityCommodityReference>> period2comms = uninitializedCommodities.stream()
                        .collect(Collectors.groupingBy(comm -> entity2period.get(comm.getEntityOid())));

        List<HistoryLoadingCallable> loadingTasks = new LinkedList<>();
        for (Map.Entry<Integer, List<EntityCommodityReference>> period2comm : period2comms.entrySet()) {
            // chunk commodities of each period by configured size
            List<List<EntityCommodityReference>> partitions = Lists
                            .partition(period2comm.getValue(), getConfig().getLoadingChunkSize());
            for (List<EntityCommodityReference> chunk : partitions) {
                loadingTasks.add(new HistoryLoadingCallable(context,
                                                            new TimeSlotLoadingTask(getStatsHistoryClient(),
                                                                                    period2comm.getKey()),
                                                            chunk));
            }
        }
        return loadingTasks;
    }

    @Override
    protected boolean hasEnoughHistoricalData(@Nonnull HistoryAggregationContext context,
                    @Nonnull List<EntityCommodityReference> commodityRefs) {
        if (super.hasEnoughHistoricalData(context, commodityRefs)) {
            return true;
        }
        final int maxObservationPeriod =
                        getApplicableEntityToPeriod(context).values().stream().mapToInt(i -> i)
                                        .max().orElse(Integer.MAX_VALUE);
        final long runningTime = getConfig().getClock().millis() - startTimestamp;
        final boolean hasHistoricalData =
                        runningTime > Duration.ofDays(maxObservationPeriod).toMillis();
        if (hasHistoricalData) {
            clearLoadingStatistic();
            getLogger().info(
                            "Data collected for '{}'ms, which is greater than '{}' days (max observation period). So, analysis will be re-enabled",
                            runningTime, maxObservationPeriod);
        }
        return hasHistoricalData;
    }

    private Map<Long, Integer> getApplicableEntityToPeriod(
                    @Nonnull HistoryAggregationContext context) {
        try {
            return topologyToApplicableEntityToPeriod.get(context.getTopologyId(),
                            () -> context.entityToSetting(this::isEntityApplicable,
                                            entity -> getConfig().getObservationPeriod(context,
                                                            entity.getOid())));
        } catch (ExecutionException e) {
            getLogger().error("Cannot get applicable entity to observation period mapping",
                            e.getCause());
        }
        return Collections.emptyMap();
    }

    @Override
    public void completeBroadcast(@Nonnull HistoryAggregationContext context)
            throws HistoryCalculationException, InterruptedException {
        updateSoldHistoricalTimeSlotValues(context);
        super.completeBroadcast(context);
        topologyToApplicableEntityToPeriod.invalidate(context.getTopologyId());
    }

    private void updateSoldHistoricalTimeSlotValues(final HistoryAggregationContext context) {
        // Retrieve all desktop pool entities with the associated observation period setting
        final Map<Long, Integer> dP2Period = context.entityToSetting(
                topologyEntity -> EntityType.DESKTOP_POOL_VALUE == topologyEntity.getEntityType(),
                entity -> getConfig().getSlots(context, entity.getOid()));

        // Gather the sold commodity references for each DP
        List<EntityCommodityFieldReference> soldFieldReferences =
                new ArrayList<>();
        for (final Long dpOid : dP2Period.keySet()) {
            ENABLED_BOUGHT_COMMODITY_TYPES.forEach(type -> {
                soldFieldReferences.add(new EntityCommodityFieldReference(dpOid,
                        TopologyDTO.CommodityType.newBuilder().setType(type.getNumber()).build(),
                        CommodityField.USED));
            });
        }
        // process each commodity reference to update the historical values
        for (EntityCommodityFieldReference commRef : soldFieldReferences) {
            Integer period = dP2Period.get(commRef.getEntityOid());
            Double[] histValue = new Double[period];
            Double util = context.getAccessor().getRealTimeValue(commRef);
            if (util == null) {
                util = 0d;
            }
            Arrays.fill(histValue, util);
            // update the historical value with the 0 list
            context.getAccessor().updateHistoryValue(commRef,
                    hv -> hv.addAllTimeSlot(Arrays.asList(histValue)),
                    TimeSlotEditor.class.getSimpleName());
        }
    }

}
