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
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.utils.HistoryStatsUtils;

/**
 * Responsible for deriving the various "ratio" properties (e.g. number of VMs per host)
 * from the market stats tables.
 *
 * <p>We don't actually save these ratios to the table. We just save the entity counts, and
 * we derive the ratios from the entity counts. This {@link FullMarketRatioProcessor} is
 * primarily responsible for:</p>
 * <ul>
 * <li>Ensuring that the {@link StatsFilter} passed to the query construction layer contains
 * the entity count commodity requests needed to derive the requested ratios.</li>
 * <li>Ensuring that the response returned to the caller does NOT contain the entity count
 * commodities if the caller didn't ask for them.</li>
 * </ul>
 *
 * <p>For example, if the user asks only for "number of VMs per host", the
 * {@link FullMarketRatioProcessor} should add commodity requests for the number of VMs and
 * the number of Hosts. Then, when processing the returned {@link Record}s, it should
 * add the ratio and remove the number of VMs and the number of Hosts (since the user didn't
 * ask for those).</p>
 *
 * <p>Additionally, if any METRICS commodities (like numHosts) are requested in the original
 * query, and if the retrieved results do not include results for those commodities, this
 * class fills them in with zero values./p>
 */
public class FullMarketRatioProcessor {

    private static final Logger logger = LogManager.getLogger();

    static final ImmutableBiMap<String, String> metricsCountSEs =
            HistoryStatsUtils.countSEsMetrics.inverse();

    /**
     * These are the filters we expect on the ratio properties.
     */
    private static final Set<String> EXPECTED_FILTER_TYPES =
            ImmutableSet.of(StringConstants.ENVIRONMENT_TYPE);

    private final RatioRecordFactory ratioRecordFactory;

    // all original commodity requests
    private final Set<String> origCommNames;
    // names of commodities appearing in original request
    private final HashSet<CommodityRequest> origCommRequests;
    // names of metrics commodities appearing in original request
    private final SetView<String> origCountMetrics;
    // ratio commodities requests in original request
    private final List<CommodityRequest> ratioCommRequests;

    private final StatsFilter statsFilter;

    private FullMarketRatioProcessor(@Nonnull final StatsFilter statsFilter,
            @Nonnull final RatioRecordFactory ratioRecordFactory) {
        this.statsFilter = statsFilter;
        this.ratioRecordFactory = Objects.requireNonNull(ratioRecordFactory);
        this.origCommRequests = Sets.newHashSet(statsFilter.getCommodityRequestsList());
        this.origCommNames = origCommRequests.stream()
                .map(CommodityRequest::getCommodityName)
                .collect(Collectors.toSet());
        this.origCountMetrics = Sets.intersection(origCommNames, HistoryStatsUtils.countSEsMetrics.values());
        this.ratioCommRequests = origCommRequests.stream()
                .filter(request -> HistoryStatsUtils.METRICS_FOR_RATIOS.containsKey(request.getCommodityName()))
                .collect(Collectors.toList());

    }

    /**
     * Get the {@link StatsFilter} to use for the actual query, with any required count
     * commodities added.
     *
     * @return stats filter enhanced with needed count commodities
     */
    @Nonnull
    public StatsFilter getFilterWithCounts() {
        if (ratioCommRequests.isEmpty()) {
            return statsFilter;
        }

        final Set<CommodityRequest> extraCommodityRequests = ratioCommRequests.stream()
                .flatMap(this::getCountCommRequestsForRatioRequest)
                .filter(countCommodityRequest -> !origCommRequests.contains(countCommodityRequest))
                .collect(Collectors.toSet());

        final Set<String> unexpectedFilters = ratioCommRequests.stream()
                .map(CommodityRequest::getPropertyValueFilterList)
                .flatMap(List::stream)
                .map(PropertyValueFilter::getProperty)
                .filter(propertyName -> !EXPECTED_FILTER_TYPES.contains(propertyName))
                .collect(Collectors.toSet());

        if (!unexpectedFilters.isEmpty()) {
            logger.error("Unexpected ratio stat filters: {} in stats filter: {}." +
                    "Returned ratios/counts may be inaccurate.", unexpectedFilters, statsFilter);
        }

        final StatsFilter.Builder filterBuilder = StatsFilter.newBuilder(statsFilter)
                .clearCommodityRequests();
        origCommRequests.stream()
                .filter(req -> !ratioCommRequests.contains(req))
                .forEach(filterBuilder::addCommodityRequests);
        filterBuilder.addAllCommodityRequests(extraCommodityRequests);
        return filterBuilder.build();
    }

    private Stream<CommodityRequest> getCountCommRequestsForRatioRequest(CommodityRequest ratioRequest) {
        return HistoryStatsUtils.METRICS_FOR_RATIOS.get(ratioRequest.getCommodityName()).stream()
                .map(metric -> substituteCommodity(ratioRequest, metric));
    }

    private CommodityRequest substituteCommodity(CommodityRequest origRequest, String newCommodityName) {
        // We use the ratio property request as a template for the count commodity
        // request, so that we retain any filters. For example, if the ratio
        // request is for the ON_PREM environment type, the underlying count
        // requests should also be for ON_PREM.
        return origRequest.toBuilder()
                .setCommodityName(newCommodityName)
                .build();
    }

    /**
     * Process the results retrieved from the database. This method will:
     * 1) Use the retrieved entity counts to calculate the requested ratio properties, and
     * insert fake "Record"s for the ratio properties into the result.
     * 2) Remove any "count" properties that were not requested by the user, and only added
     * to support the ratio calculation from the results.
     *
     * @param results          The results retrieved from the database using the filter returned by
     *                         {@link FullMarketRatioProcessor#getFilterWithCounts()}.
     * @param defaultTimeStamp The default time stamp, used to generate relevant timestamps
     *                         in the case missing entries need to be filled in.
     * @return A (shallow) copy of the results with the extra "fake" ratio properties, and
     * without the non-user-requested "count" properties.
     */
    @Nonnull
    public List<Record> processResults(@Nonnull final List<? extends Record> results,
            @Nonnull final Timestamp defaultTimeStamp) {
        // if original request asked for any count metrics we need to check if they're missing;
        // if original request asked for any ratio metrics we need to compute them
        if (origCountMetrics.isEmpty() && ratioCommRequests.isEmpty()) {
            // if neither of the above conditions holds, take an early exit
            // and avoid computing our commodities map
            return (List<Record>)results;
        }

        // this is what we will actually send back for the query response
        final List<Record> answer = new ArrayList<>(results.size());

        // The results we get back from the query above contain multiple entity counts
        // associated with snapshot times. We want, for every snapshot that has entity
        // counts, to create the ratio properties. The resulting data structure
        // is a map of timestamp -> map of entity count -> num of entities.
        final Map<Timestamp, Map<String, Integer>> entityCountsByTimeAndType =
                getEntityCountsByTimeAndType(results, defaultTimeStamp);

        // Add originally requested properties to the answer.
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
        results.forEach(record -> {
            if (origCommNames.contains(record.getValue(PROPERTY_TYPE, String.class))) {
                answer.add(record);
            }
        });
        // create and add records for requested ratio stats
        ratioCommRequests.stream()
                .flatMap(ratioComm ->
                        computeRatioCommoditiesRecords(entityCountsByTimeAndType))
                .forEach(answer::add);
        // create zero-valued records for requested but missing metrics commodities
        answer.addAll(getMissingCountRecords(entityCountsByTimeAndType));
        return answer;
    }

    /**
     * Compute a utility structure form the records returned from the augmented query.
     *
     * @param records          augmented query result
     * @param defaultTimeStamp timestamp to use if we for an empty structure if we got nothing
     * @return map of timestamp -> commodity type name -> value computed from query result
     */
    private Map<Timestamp, Map<String, Integer>> getEntityCountsByTimeAndType(
            final List<? extends Record> records, @Nonnull final Timestamp defaultTimeStamp) {

        final Map<Timestamp, Map<String, Integer>> result = new HashMap<>();
        records.forEach(record -> {
            // containsValue is efficient in a BiMap.
            final String entityTypeToCount = HistoryStatsUtils.countSEsMetrics.inverse()
                    .get(record.getValue(PROPERTY_TYPE, String.class));
            if (entityTypeToCount != null) {
                final Timestamp statTime = record.getValue(SNAPSHOT_TIME, Timestamp.class);
                final Map<String, Integer> snapshotCounts =
                        result.computeIfAbsent(statTime, key -> new HashMap<>());

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
                snapshotCounts.merge(
                        entityTypeToCount,
                        // Entity counts should be discrete numbers.
                        Math.round(record.getValue(AVG_VALUE, Float.class)),
                        (a, b) -> a + b);
            }
        });
        // Handle the case where no records were found -- we want to initialize all requested
        // entity counts and ratio counts to zero in this case!
        if (result.isEmpty()) {
            result.put(defaultTimeStamp, Collections.emptyMap());
        }
        return result;
    }

    /**
     * Create commodities records for every requested ratio commodity.
     *
     * @param entityCountsByTimeAndType map of (timestamp -> commodityName -> count) from query
     * @return records for ratio commodities
     */
    private Stream<Record> computeRatioCommoditiesRecords(
            Map<Timestamp, Map<String, Integer>> entityCountsByTimeAndType) {
        // Go through the entity counts by time, and for each timestamp create
        // ratio properties based on the various counts.
        return entityCountsByTimeAndType.entrySet().stream()
                .flatMap(entry -> {
                    final Timestamp snapshotTime = entry.getKey();
                    final Map<String, Integer> entityCounts = entry.getValue();
                    return ratioCommRequests.stream()
                            .map(ratioComm -> ratioRecordFactory.makeRatioRecord(
                                    snapshotTime, ratioComm.getCommodityName(), entityCounts));
                });
    }


    /**
     * Check for any requested metrics commodities that are missing from the results,
     * and create zero-valued records for them.
     *
     * @param entityCountsByTimeAndType map (timestamp -> commodityName -> count) from query result
     * @return created records
     */
    private List<Record> getMissingCountRecords(
            final Map<Timestamp, Map<String, Integer>> entityCountsByTimeAndType) {
        List<Record> result = new ArrayList<>();
        // Go through the entity counts by time, and for each timestamp check if any commodity
        // count stats are missing--fill these in with a stat set to zero.
        entityCountsByTimeAndType.forEach((snapshotTime, entityCounts) ->
                origCountMetrics.stream()
                        .filter(HistoryStatsUtils.countSEsMetrics::containsValue)
                        .filter(comm -> {
                            return !entityCounts.containsKey(metricsCountSEs.get(comm));
                        })
                        .map(comm -> createEmptyEntityCountRecord(snapshotTime, comm))
                        .forEach(result::add));
        return result;
    }

    /**
     * Create a stat record to represent an entity type count of zero.
     *
     * <p>This is used in place of simply omitting the entity stat count.</p>
     *
     * @param snapshotTimestamp the snapshot time to use for this record
     * @param commodityName     the entity type count commodity name requested
     * @return a stat record represeting an entity type count of zero
     */
    private Record createEmptyEntityCountRecord(@Nonnull final Timestamp snapshotTimestamp,
            @Nonnull final String commodityName) {
        final PmStatsLatestRecord record = new PmStatsLatestRecord();
        record.setSnapshotTime(snapshotTimestamp);
        record.setPropertyType(commodityName);
        record.setAvgValue(0D);
        record.setRelation(RelationType.METRICS);
        return record;
    }

    /**
     * Factory class for {@link FullMarketRatioProcessor}s, for dependency injection/isolation
     * purposes.
     */
    public static class FullMarketRatioProcessorFactory {
        private final RatioRecordFactory ratioRecordFactory;

        /**
         * Create a new factory instance.
         *
         * @param ratioRecordFactory for creating records with ratio properties
         */
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
