package com.vmturbo.cost.component.history;

import java.util.Calendar;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Provides access to the history stats gRPC service.
 */
public class HistoricalStatsServiceImpl implements HistoricalStatsService {
    private static final Logger logger = LogManager.getLogger(HistoricalStatsServiceImpl.class);

    /**
     * The stats history gRPC client.
     */
    private StatsHistoryServiceBlockingStub statsHistoryServiceClient;

    /**
     * Creates a new HistoricalStatsServiceImpl.
     *
     * @param statsHistoryServiceClient The stats history gRPC client.
     */
    public HistoricalStatsServiceImpl(StatsHistoryServiceBlockingStub statsHistoryServiceClient) {
        this.statsHistoryServiceClient = statsHistoryServiceClient;
    }

    @Override
    public List<Stats.EntityStats> getHistoricalStats(List<Long> oids, List<String> commodities, int numberOfDays) {
        logger.info("Retrieving historical stats for OIDs: {}", oids);

        // Build the scope of our request
        Stats.EntityStatsScope scope = Stats.EntityStatsScope.newBuilder()
                .setEntityList(Stats.EntityStatsScope.EntityList.newBuilder().addAllEntities(oids).build())
                .build();

        // Build our StatsFilter
        StatsFilter statsFilter = buildStatsFilter(commodities, numberOfDays);

        // Execute the gRPC call
        GetEntityStatsResponse response = statsHistoryServiceClient.getEntityStats(GetEntityStatsRequest
                .newBuilder()
                .setScope(scope)
                .setFilter(statsFilter)
                .build());

        // Handle the results
        logger.info("Received historical stats");
        return response.getEntityStatsList();
    }

    /**
     * Builds a StatsFilter for the specified commodity list and number of days.
     *
     * @param commodities  A list of commodities for which to retrieve statistics.
     * @param numberOfDays The number of historical days for which to retrieve statistics.
     * @return A StatsFilter instance configured with the specified commodities and number of historical days.
     */
    private StatsFilter buildStatsFilter(List<String> commodities, int numberOfDays) {
        // Determine start/end date
        final Calendar periodEndTimeCal = Calendar.getInstance();
        final Calendar periodBeginTimeCal = getBeginTime(periodEndTimeCal, numberOfDays);

        // Build our Stats filter builder with start and add dates
        final StatsFilter.Builder builder = StatsFilter.newBuilder()
                .setStartDate(periodBeginTimeCal.getTimeInMillis())
                .setEndDate(periodEndTimeCal.getTimeInMillis());

        // Add all of the requested commodities
        commodities.forEach(commodity -> builder.addCommodityRequests(StatsFilter.CommodityRequest.newBuilder().setCommodityName(commodity).build()));

        // Build the StatsFilter
        return builder.build();
    }

    /**
     * Returns a Calendar instance representing 12:00 am on the first day of a
     * time period that is offset a given number of days from a specified time
     * point.
     *
     * @param endTime the end of the period
     * @param numDays the number of days to the beginning of the period
     * @return the beginning of the period
     */
    private Calendar getBeginTime(Calendar endTime, int numDays) {
        // calculate the day at the beginning of the period
        Calendar beginTime = (Calendar) endTime.clone();
        beginTime.add(Calendar.DAY_OF_YEAR, -numDays);

        // zero out the time portion to the beginning of the day
        beginTime.set(Calendar.HOUR_OF_DAY, 0);
        beginTime.set(Calendar.MINUTE, 0);
        beginTime.set(Calendar.SECOND, 0);
        beginTime.set(Calendar.MILLISECOND, 0);

        return beginTime;
    }
}
