package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.utils.HistoryStatsUtils;

/**
 * Responsible for deriving the various "ratio" properties (e.g. number of VMs per host)
 * from the market stats tables.
 * <p>
 * We don't actually save these ratios to the table. We just save the entity counts, and
 * we derive the ratios from the entity counts. This {@link FullMarketRatioProcessor} is
 * responsible for:
 * 1) Ensuring that the {@link StatsFilter} passed to the query construction layer contains
 *    the entity count commodity requests needed to derive the requested ratios.
 * 2) Ensuring that the response returned to the caller does NOT contain the entity count
 *    commodities if the caller didn't ask for them.
 * <p>
 * For example, if the user asks only for "number of VMs per host", the
 * {@link FullMarketRatioProcessor} should add commodity requests for the number of VMs and
 * the number of Hosts. Then, when processing the returned {@link Record}s, it should
 * add the ratio and remove the number of VMs and the number of Hosts (since the user didn't
 * ask for those).
 */
public class FullMarketRatioProcessor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * These are the filters we expect on the ratio properties.
     */
    private static final Set<String> EXPECTED_FILTER_TYPES =
        ImmutableSet.of(StringConstants.ENVIRONMENT_TYPE);

    private final RatioRecordFactory ratioRecordFactory;

    private final StatsFilter statsFilterWithCounts;

    /**
     * The set of commodities requested in the original stats filter.
     */
    private final Set<String> requestedComms;

    /**
     * The set of requested "ratio" props (see: {@link HistoryStatsUtils#countPerSEsMetrics}).
     */
    private final Set<String> requestedRatioProps;

    private FullMarketRatioProcessor(@Nonnull final StatsFilter statsFilter,
                                     @Nonnull final RatioRecordFactory ratioRecordFactory) {
        this.ratioRecordFactory = Objects.requireNonNull(ratioRecordFactory);

        final Set<CommodityRequest> allCommodityRequests =
            Sets.newHashSet(statsFilter.getCommodityRequestsList());

        // Note - it may be that we have multiple commodity requests for the same commodity
        // but different filters. We lose that information here.
        requestedComms = allCommodityRequests.stream()
            .map(CommodityRequest::getCommodityName)
            .collect(Collectors.toSet());

        requestedRatioProps = Sets.intersection(requestedComms, HistoryStatsUtils.countPerSEsMetrics);

        final Set<String> unexpectedFilters = new HashSet<>();

        final Set<CommodityRequest> extraCommodityRequests = allCommodityRequests.stream()
            .flatMap(request -> {
                // Record unexpected filters.
                request.getPropertyValueFilterList().stream()
                    .map(PropertyValueFilter::getProperty)
                    .filter(propertyName -> !EXPECTED_FILTER_TYPES.contains(propertyName))
                    .forEach(unexpectedFilters::add);

                return HistoryStatsUtils.METRICS_FOR_RATIOS.getOrDefault(request.getCommodityName(),
                    Collections.emptySet()).stream()
                    .map(requiredCountCommodity ->
                        // We use the ratio property request as a template for the count commodity
                        // request, so that we retain any filters. For example, if the ratio
                        // request is for the ON_PREM environment type, the underlying count
                        // requests should also be for ON_PREM.
                        request.toBuilder()
                            .setCommodityName(requiredCountCommodity)
                            .build());
            })
            .filter(countCommodityRequest -> !allCommodityRequests.contains(countCommodityRequest))
            .collect(Collectors.toSet());

        if (!unexpectedFilters.isEmpty()) {
            logger.error("Unexpected ratio stat filters: {} in stats filter: {}." +
                "Returned ratios/counts may be inaccurate.", unexpectedFilters, statsFilter);
        }

        if (!requestedRatioProps.isEmpty()) {
            final StatsFilter.Builder filterBuilder = StatsFilter.newBuilder(statsFilter)
                .clearCommodityRequests();
            allCommodityRequests.stream()
                .filter(req -> !requestedRatioProps.contains(req.getCommodityName()))
                .forEach(filterBuilder::addCommodityRequests);
            filterBuilder.addAllCommodityRequests(extraCommodityRequests);
            this.statsFilterWithCounts = filterBuilder.build();
        } else {
            this.statsFilterWithCounts = statsFilter;
        }
    }

    /**
     * Get the {@link StatsFilter} to use for the actual query, with any required count
     * commodities added.
     */
    @Nonnull
    public StatsFilter getFilterWithCounts() {
        return statsFilterWithCounts;
    }

    /**
     * Process the results retrieved from the database. This method will:
     * 1) Use the retrieved entity counts to calculate the requested ratio properties, and
     *    insert fake "Record"s for the ratio properties into the result.
     * 2) Remove any "count" properties that were not requested by the user, and only added
     *    to support the ratio calculation from the results.
     *
     * @param results The results retrieved from the database using the filter returned by
     *                {@link FullMarketRatioProcessor#getFilterWithCounts()}.
     * @return A (shallow) copy of the results with the extra "fake" ratio properties, and
     *         without the non-user-requested "count" properties.
     */
    @Nonnull
    public List<Record> processResults(@Nonnull final List<? extends Record> results) {
        if (requestedRatioProps.isEmpty()) {
            return new ArrayList<>(results);
        }

        // The results we get back from the query above contain multiple entity counts
        // associated with snapshot times. We want, for every snapshot that has entity
        // counts, to create the ratio properties. The resulting data structure
        // is a map of timestamp -> map of entity count -> num of entities.
        final Map<Timestamp, Map<String, Integer>> entityCountsByTimeAndType = new HashMap<>();

        // Go through all the returned records, and initialize the entity counts.
        // The number of records is not expected to be super-high, so this pass
        // won't be very expensive.
        final List<Record> answer = new ArrayList<>(results.size());
        results.forEach(record -> {
            final String propName = record.getValue(PROPERTY_TYPE, String.class);
            // containsValue is efficient in a BiMap.
            final String entityTypeToCount = HistoryStatsUtils.countSEsMetrics.inverse().get(propName);
            if (entityTypeToCount != null) {
                final Timestamp statTime = record.getValue(SNAPSHOT_TIME, Timestamp.class);
                final Map<String, Integer> snapshotCounts =
                    entityCountsByTimeAndType.computeIfAbsent(statTime, key -> new HashMap<>());

                // Metrics may appear more than once per SNAPSHOT_TIME - for example, we record
                // the counts separately for ON_PREM and CLOUD environment types.
                // If the same metric appears more than once, we combine the values.
                //
                // Note (roman, Feb 7 2019): Before the environment type change we assumed that
                // no metric would appear more than once. With the environment type change,
                // we know that the right thing to do is to combine the values. However, in
                // the future there may be other commodities where the right thing to do is
                // to drop one of the values, or to average them. We will cross that bridge
                // when we get there!
                snapshotCounts.compute(entityTypeToCount, (k, existing) -> {
                    // Entity counts should be discrete numbers.
                    final int val = Math.round(record.getValue(AVG_VALUE, Float.class));
                    if (existing == null) {
                        return val;
                    } else {
                        return existing + val;
                    }
                });
            }

            // Only add the requested properties to the answer.
            //
            // We don't expect or handle complex filter scenarios for count stats - for example,
            // we don't expect a request that contains "numVMs : CLOUD, vmPerHost ON_PREM".
            // If we get a request like that, the following logic will mostly work, but we
            // may see wrong numbers for the VM count. Why? Because we will add the "numVMs : ON_PREM"
            // row to the request, but we won't remove the "numVMs : ON_PREM" results.
            //
            // If we want to handle this case properly we need to be able to apply the filters
            // in the CommodityRequest to the records here. For now we just assume we won't get
            // requests like that. UI requests are generally straightforward, and usually
            // share the same filters.
            if (requestedComms.contains(propName)) {
                answer.add(record);
            }
        });

        // Go through the entity counts by time, and for each timestamp create
        // ratio properties based on the various counts.
        entityCountsByTimeAndType.forEach((snapshotTime, entityCounts) ->
            requestedRatioProps.forEach(ratioPropName -> {
                final Record ratioRecord =
                    ratioRecordFactory.makeRatioRecord(snapshotTime, ratioPropName, entityCounts);
                answer.add(ratioRecord);
        }));

        return answer;
    }


    /**
     * Factory class for {@link FullMarketRatioProcessor}s, for dependency injection/isolation
     * purposes.
     */
    public static class FullMarketRatioProcessorFactory {
        private final RatioRecordFactory ratioRecordFactory;

        public FullMarketRatioProcessorFactory(final RatioRecordFactory ratioRecordFactory) {
            this.ratioRecordFactory = ratioRecordFactory;
        }

        /**
         * Create a new {@link FullMarketRatioProcessor}.
         *
         * @param statsFilter The original filter in the request coming from the user.
         * @return The {@link FullMarketRatioProcessor}.
         */
        @Nonnull
        public FullMarketRatioProcessor newProcessor(@Nonnull final StatsFilter statsFilter) {
            return new FullMarketRatioProcessor(statsFilter, ratioRecordFactory);
        }
    }
}
