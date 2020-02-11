package com.vmturbo.topology.processor.history.timeslot;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.AbstractStatsLoadingTask;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.IHistoryLoadingTask;

/**
 * Loader of time slot data from historydb.
 */
public class TimeSlotLoadingTask extends AbstractStatsLoadingTask<TimeslotHistoricalEditorConfig, List<Pair<Long, StatRecord>>>
        implements IHistoryLoadingTask<TimeslotHistoricalEditorConfig, List<Pair<Long, StatRecord>>> {
    private static final Logger logger = LogManager.getLogger();

    private final StatsHistoryServiceBlockingStub statsHistoryClient;
    private final int periodDays;

    /**
     * Construct the timeslot (hourly) data loading task.
     *
     * @param statsHistoryClient history client connection
     */
    public TimeSlotLoadingTask(@Nonnull StatsHistoryServiceBlockingStub statsHistoryClient) {
        this.statsHistoryClient = statsHistoryClient;
        this.periodDays = (int)EntitySettingSpecs.MaxObservationPeriodDesktopPool.getSettingSpec()
                        .getNumericSettingValueType().getDefault();
    }

    /**
     * Construct the timeslot (hourly) data loading task.
     *
     * @param statsHistoryClient history client connection
     * @param periodDays period for which to request data
     */
    public TimeSlotLoadingTask(@Nonnull StatsHistoryServiceBlockingStub statsHistoryClient, int periodDays) {
        this.statsHistoryClient = statsHistoryClient;
        this.periodDays = periodDays;
    }

    @Override
    @Nonnull
    public Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>>
        load(@Nonnull Collection<EntityCommodityReference> commodities, @Nonnull TimeslotHistoricalEditorConfig config)
                    throws HistoryCalculationException {
        Stopwatch sw = Stopwatch.createStarted();

        long now = config.getClock().millis();
        long lastDay = now / TimeInMillisConstants.DAY_LENGTH_IN_MILLIS * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS;
        long lastHour = now / TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS;
        GetEntityStatsRequest request = createStatsRequest(commodities,
                   lastDay - periodDays * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS,
                   lastHour,
                   TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS);
        long records = 0;

        Map<EntityCommodityFieldReference, List<Pair<Long, StatRecord>>> fields2records = new HashMap<>();
        try {
            GetEntityStatsResponse response = statsHistoryClient.getEntityStats(request);

            for (EntityStats stats : response.getEntityStatsList()) {
                // assume snapshots in time order
                for (StatSnapshot snapshot : stats.getStatSnapshotsList()) {
                    for (StatRecord record : snapshot.getStatRecordsList()) {
                        Long provider = null;
                        if (record.hasProviderUuid()) {
                            provider = Long.parseLong(record.getProviderUuid());
                        }
                        UICommodityType uiCommType = UICommodityType.fromStringIgnoreCase(record.getName());
                        if (uiCommType == UICommodityType.UNKNOWN) {
                            logger.warn("Received unknown statistic for entity {}: {}", stats.getOid(), record);
                            continue;
                        }
                        CommodityType.Builder ct = CommodityType.newBuilder().setType(uiCommType.typeNumber());
                        if (record.hasStatKey()) {
                            ct.setKey(record.getStatKey());
                        }
                        EntityCommodityFieldReference field =
                                    new EntityCommodityFieldReference(stats.getOid(), ct.build(),
                                                                      provider, CommodityField.USED);
                        ++records;
                        fields2records.computeIfAbsent(field, f -> new LinkedList<>())
                                        .add(Pair.create(snapshot.getSnapshotDate(), record));
                    }
                }
            }
        } catch (RuntimeException e) {
            // grpc throws all kinds of things
            throw new HistoryCalculationException("Failed to get hourly statistics for timeslot analysis", e);
        }

        // initialize with empty data commodities that have no db values
        for (EntityCommodityReference commRef : commodities) {
            EntityCommodityFieldReference field =
                            new EntityCommodityFieldReference(commRef, CommodityField.USED);
            fields2records.putIfAbsent(field, Collections.emptyList());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Timeslot caching: queried {} hourly stat records for {} commodities and () days in {}",
                         records, fields2records.size(), periodDays, sw);
        }
        return fields2records;
    }
}
