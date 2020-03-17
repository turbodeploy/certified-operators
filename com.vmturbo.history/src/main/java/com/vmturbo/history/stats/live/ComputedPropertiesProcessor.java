package com.vmturbo.history.stats.live;

import static com.vmturbo.components.common.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.components.common.utils.StringConstants.INTERNAL_NAME;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.RECORDED_ON;
import static com.vmturbo.components.common.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.components.common.utils.StringConstants.VALUE;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsLatest.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.stats.live.PropertyType.Category.Headroom;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsLatestRecord;

/**
 * Responsible for handling various computed properties - properties that may be requested in
 * stats requests but are not present in the history tables.
 *
 * <p>This class enables delivery of these properties via a two-step process:</p>
 * <ul>
 *     <li>
 *         Prior to retrieving records for the query, we alter the {@link StatsFilter} passed to
 *         the query construction layer so that it requests all the commodities needed to compute
 *         any computed properties that appear in the original request.
 *      </li>
 *     <li>
 *         After records have been retrieved, we post-process and:
 *         <ul>
 *             <li>Add records for requested computed properties</li>
 *             <li>Remove records for properties added in step 1</li>
 *         </ul>
 *     </li>
 * </ul>
 *
 * <p>For example, if the user asks only for "number of VMs per host", the
 * {@link ComputedPropertiesProcessor} should add commodity requests for the number of VMs and
 * the number of Hosts. Then, when processing the returned {@link Record}s, it should
 * add the ratio and remove the number of VMs and the number of Hosts (since the user didn't
 * ask for those).</p>
 *
 * <p>Additionally, if any METRICS commodities (like numHosts) are requested in the original
 * query, and if the retrieved results do not include results for those commodities, this
 * class fills them in with zero values./p>
 */
public class ComputedPropertiesProcessor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * These are the filters we expect on the ratio properties.
     */
    private static final Set<String> EXPECTED_FILTER_TYPES =
            ImmutableSet.of(StringConstants.ENVIRONMENT_TYPE);

    // all original commodity requests
    private final Set<CommodityRequest> originalRequests;
    // all originally requested property types
    private final Set<PropertyType> originalPropertyTypes;
    // requested property types that have defaults
    private final Set<PropertyType> countMetricPropertyTypes;
    // original requests involving computed propertie
    private final List<CommodityRequest> computedRequests;
    // requested property types that are computed
    private final List<PropertyType> computedPropertyTypes;

    private final StatsFilter statsFilter;
    private final RecordsProcessor recordsProcessor;

    /**
     * Create a new instance to manage computed properties for a query.
     *
     * @param statsFilter      the stats filter from the query, identifying requested properties
     * @param recordsProcessor a {@link RecordsProcessor} appropriate for the retrieved records
     */
    public ComputedPropertiesProcessor(@Nonnull final StatsFilter statsFilter,
            @Nonnull final RecordsProcessor recordsProcessor) {
        this.statsFilter = statsFilter;
        this.recordsProcessor = recordsProcessor;
        this.originalRequests = Sets.newHashSet(statsFilter.getCommodityRequestsList());
        originalPropertyTypes = originalRequests.stream()
                .map(CommodityRequest::getCommodityName)
                .map(PropertyType::named)
                .collect(Collectors.toSet());
        this.countMetricPropertyTypes = originalPropertyTypes.stream()
                .filter(PropertyType::isCountMetric)
                .collect(Collectors.toSet());
        this.computedRequests = originalRequests.stream()
                .filter(req -> PropertyType.named(req.getCommodityName()).isComputed())
                .collect(Collectors.toList());
        this.computedPropertyTypes = originalPropertyTypes.stream()
                .filter(PropertyType::isComputed)
                .collect(Collectors.toList());
    }

    /**
     * Get the {@link StatsFilter} to use for the actual query, with any required count
     * commodities added.
     *
     * @return stats filter enhanced with needed count commodities
     */
    @Nonnull
    public StatsFilter getAugmentedFilter() {
        if (computedPropertyTypes.isEmpty()) {
            return statsFilter;
        }

        final Set<CommodityRequest> addedCommodityRequests = computedRequests.stream()
                .flatMap(this::getAddedRequestsForComputedProperty)
                .filter(addedRequest -> !originalRequests.contains(addedRequest))
                .collect(Collectors.toSet());

        final Set<String> unexpectedFilters = computedRequests.stream()
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
        originalRequests.stream()
                .filter(req -> !computedRequests.contains(req))
                .forEach(filterBuilder::addCommodityRequests);
        filterBuilder.addAllCommodityRequests(addedCommodityRequests);
        return filterBuilder.build();
    }

    private Stream<CommodityRequest> getAddedRequestsForComputedProperty(CommodityRequest computedRequest) {
        final PropertyType computedProperty = PropertyType.named(computedRequest.getCommodityName());
        return computedProperty.getOperands().stream()
                .map(prereq -> substituteCommodity(computedRequest, prereq));

    }

    private CommodityRequest substituteCommodity(CommodityRequest origRequest, PropertyType newPropertyType) {
        // We use the ratio property request as a template for the count commodity
        // request, so that we retain any filters. For example, if the ratio
        // request is for the ON_PREM environment type, the underlying count
        // requests should also be for ON_PREM.
        return origRequest.toBuilder()
                .setCommodityName(newPropertyType.getName())
                .build();
    }

    /**
     * Process the results retrieved from the database.
     *
     * <p>This method will:</p>
     * <ol>
     *     <li>
     *         Use the retrieved entity counts to calculate the requested ratio properties, and
     *         insert fake "Record"s for the ratio properties into the result.     *
     *     </li>
     *     <li>
     *         Remove any "count" properties that were not requested by the user, and only added
     *         to support the ratio calculation from the results.
     *     </li>
     *     <li>
     *         If any count properties are missing from results, add zero records for them.
     *     </li>
     * </ol>
     *
     * @param results          The results retrieved from the database using the filter returned by
     *                         {@link ComputedPropertiesProcessor#getAugmentedFilter()}.
     * @param defaultTimestamp timestamp to use for any zero-count records added when there were
     *                         no count records in the processed data, or null to omit such records
     * @return A copy of the query results, with added records for computed properties, and with
     * records for any added metrics properties removed
     */
    @Nonnull
    public List<Record> processResults(@Nonnull List<? extends Record> results,
            @Nullable Timestamp defaultTimestamp) {
        // get rid of captured type
        List<Record> castResults = (List<Record>)results;

        // if original request asked for any count metrics we need to check if they're missing;
        // if original request asked for any ratio metrics we need to compute them
        if (countMetricPropertyTypes.isEmpty() && computedPropertyTypes.isEmpty()) {
            // if neither of the above conditions holds, take an early exit
            // and avoid computing our commodities map
            return castResults;
        }

        // this is what we will actually send back for the query response
        final List<Record> answer = new ArrayList<>(castResults.size());

        // split the result data into a map snapshot_time -> propertyType -> records
        final Map<Object, Multimap<PropertyType, Record>> recordsByGroupAndType =
                splitRecordsByGroupAndType(castResults);


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
        castResults.forEach(record -> {
            if (originalPropertyTypes.contains(
                    PropertyType.named(record.getValue(PROPERTY_TYPE, String.class)))) {
                answer.add(record);
            }
        });
        // create and add records for requested ratio stats
        createComputedPropertyRecords(recordsByGroupAndType).forEach(answer::add);
        // create zero-valued records for requested but missing metrics commodities
        getMissingCountRecords(recordsByGroupAndType, defaultTimestamp).forEach(answer::add);
        return answer;
    }

    /**
     * Compute a utility structure form the records returned from the augmented query.
     *
     * @param records augmented query result
     * @return map of timestamp -> commodity type name -> value computed from query result
     */
    private Map<Object, Multimap<PropertyType, Record>> splitRecordsByGroupAndType(
            final List<Record> records) {
        final Map<Object, Multimap<PropertyType, Record>> result = new HashMap<>();
        for (final Record record : records) {
            final Object groupKey = recordsProcessor.getRecordGroupKey(record);
            final PropertyType propertyType = PropertyType.named(record.get(PROPERTY_TYPE, String.class));
            result.computeIfAbsent(groupKey, key -> MultimapBuilder.hashKeys().arrayListValues().build())
                    .put(propertyType, record);
        }
        return result;
    }

    /**
     * Create commodities records for every requested ratio commodity.
     *
     * @param recordsByGroupAndType response records, mapped by whatever the {@link RecordsProcessor}
     *                              uses for group key, and by property type
     * @return records for computed propertiesx
     */
    private List<Record> createComputedPropertyRecords(
            Map<Object, Multimap<PropertyType, Record>> recordsByGroupAndType) {
        List<Record> results = new ArrayList();
        // Go through the entity counts by time, and for each timestamp create
        // ratio properties based on the various counts.
        for (Entry<Object, Multimap<PropertyType, Record>> groupEntry : recordsByGroupAndType.entrySet()) {
            final Object groupKey = groupEntry.getKey();
            for (PropertyType computedPropertyType : computedPropertyTypes) {
                Double value = compute(computedPropertyType, groupEntry.getValue());
                results.add(recordsProcessor.createRecord(groupKey, computedPropertyType, value));
            }
        }
        return results;
    }

    private Double compute(PropertyType propertyType, Multimap<PropertyType, Record> data) {
        return propertyType.compute(
                propertyType.getOperands().stream()
                        .map(prereq -> getValue(data.get(prereq)))
                        .collect(Collectors.toList()));
    }

    private double getValue(Collection<Record> records) {
        return records.stream()
                .collect(Collectors.summingDouble(recordsProcessor::getValue));

    }

    /**
     * Check for any requested metrics commodities that are missing from the results,
     * and create zero-valued records for them.
     *
     * @param recordsByGroupAndType map (timestamp -> commodityName -> count) from query result
     * @param defaultTimestamp   timestamp to use for these records, or null to skip creation
     * @return created records
     */
    private List<Record> getMissingCountRecords(
            final Map<Object, Multimap<PropertyType, Record>> recordsByGroupAndType,
            @Nullable final Timestamp defaultTimestamp) {
        List<Record> result = new ArrayList<>();
        if (defaultTimestamp != null) {

            // Go through the entity counts by time, and for each timestamp check if any commodity
            // count stats are missing--fill these in with a stat set to zero.
            final Set<PropertyType> propertyTypesToCompute =
                    Sets.intersection(originalPropertyTypes, countMetricPropertyTypes);
            if (!recordsByGroupAndType.isEmpty()) {
                recordsByGroupAndType.forEach((groupKey, entityCounts) ->
                        propertyTypesToCompute.stream()
                                .filter(metric -> !entityCounts.containsKey(metric))
                                .map(metric -> recordsProcessor.createRecord(
                                        groupKey, metric, 0.0))
                                .forEach(result::add));
            } else {
                propertyTypesToCompute.stream()
                        .map(metric -> recordsProcessor.createRecord(metric, 0.0, defaultTimestamp))
                        .filter(Objects::nonNull)
                        .forEach(result::add);
            }
        }
        return result;
    }

    /**
     * Interface for a utility used by {@link ComputedPropertiesProcessor}.
     *
     * <p>Different instances will be required for different underlying record designs.</p>
     *
     * @param <GroupKeyType> type representing a "group key" for grouping of records
     */
    public interface RecordsProcessor<GroupKeyType> {

        /**
         * Compute the group key for the given record.
         *
         * @param record record
         * @return record group key
         */
        GroupKeyType getRecordGroupKey(Record record);

        /**
         * Create a record with the given group key, and with the given property type and value.
         *
         * @param recordGroupKey group key for new record
         * @param propertyType   property type
         * @param value          value for that property
         * @return the new record
         */
        Record createRecord(GroupKeyType recordGroupKey, PropertyType propertyType, Number value);

        /**
         * Create a record with the given timestamp, property type, and value.
         *
         * <p>This is used in the case of filling in zero-valued records for count metrics when
         * the retrieved results come up empty. A full group key is not available in this case.</p>
         *
         * @param propertyType property type
         * @param value        value for that property
         * @param timestamp    timestamp for created record
         * @return the new record
         */
        Record createRecord(PropertyType propertyType, Number value, Timestamp timestamp);

        /**
         * Get the "value" from the given record.
         *
         * @param record the record
         * @return its value
         */
        double getValue(Record record);
    }

    /**
     * Records processor for entity stats records.
     *
     * <p>These all have a common structure, so a single records processor can be used for all.</p>
     *
     * <p>We use the record timestamp as a group key for grouping purposes.</p>
     */
    public static class StatsRecordsProcessor implements RecordsProcessor<Timestamp> {

        // we can use these fields for any entity stats tables or market stats table
        private static final Field<Timestamp> snapshotTimeField = VM_STATS_LATEST.SNAPSHOT_TIME;
        private static final Field<String> propertyTypeField = VM_STATS_LATEST.PROPERTY_TYPE;
        private static final Field<Double> avgValueField = VM_STATS_LATEST.AVG_VALUE;
        private static final Field<RelationType> relationField = VM_STATS_LATEST.RELATION;

        private Supplier<Record> recordSupplier;


        /**
         * Create a new instance.
         *
         * @param recordSupplier a supplier of new, empty records appropriate for this processor.
         */
        public StatsRecordsProcessor(Supplier<Record> recordSupplier) {
            this.recordSupplier = recordSupplier;
        }

        /**
         * Create a new instance, that uses a {@link Table} object as a record supplier.
         *
         * @param table jOOQ table
         */
        public StatsRecordsProcessor(Table<?> table) {
            this(table::newRecord);
        }

        @Override
        public Timestamp getRecordGroupKey(final Record record) {
            return record.get(SNAPSHOT_TIME, Timestamp.class);
        }

        @Override
        public Record createRecord(
                final Timestamp recordGroupKey, final PropertyType propertyType, final Number value) {
            final Record record = recordSupplier.get();
            record.set(snapshotTimeField, recordGroupKey);
            record.set(propertyTypeField, propertyType.getName());
            record.set(avgValueField, value.doubleValue());
            record.set(relationField, RelationType.METRICS);
            return record;
        }

        @Override
        public Record createRecord(
                final PropertyType propertyType, final Number value, final Timestamp timestamp) {
            return createRecord(timestamp, propertyType, value);
        }

        @Override
        public double getValue(final Record record) {
            return record.get(AVG_VALUE, Double.class);
        }
    }

    /**
     * Records procesor that an be used with cluster stats tables.
     *
     * <p>We use the combination of the record timestamp and the cluster id as the group key, for
     * grouping purposes. And, when when the property type is a headroom property, we also include
     * the property subtype in the group key.</p>
     */
    public static class ClusterRecordsProcessor
            implements RecordsProcessor<Triple<Timestamp, String, String>> {
        @Override
        public Triple<Timestamp, String, String> getRecordGroupKey(final Record record) {
            final PropertyType propertyType = PropertyType.named(record.get(PROPERTY_TYPE, String.class));
            return Triple.of(
                    record.get(RECORDED_ON, Timestamp.class),
                    record.get(INTERNAL_NAME, String.class),
                    propertyType.isInCategory(Headroom) ? record.get(PROPERTY_SUBTYPE, String.class) : "");
        }

        @Override
        public Record createRecord(final Triple<Timestamp, String, String> recordGroupKey,
                final PropertyType propertyType, final Number value) {
            final ClusterStatsLatestRecord record = CLUSTER_STATS_LATEST.newRecord();
            record.setRecordedOn(recordGroupKey.getLeft());
            record.setInternalName(recordGroupKey.getMiddle());
            record.setPropertySubtype(recordGroupKey.getRight());
            record.setPropertyType(propertyType.getName());
            record.setValue(value.doubleValue());
            return record;
        }

        @Override
        public Record createRecord(
                final PropertyType propertyType, final Number value, final Timestamp timestamp) {
            throw new UnsupportedOperationException("Cannot create record without cluster id");
        }

        @Override
        public double getValue(final Record record) {
            return record.get(VALUE, Double.class);
        }
    }

    /**
     * Interface for a {@link ComputedPropertiesProcessorFactory} factory.
     */
    public interface ComputedPropertiesProcessorFactory {

        /**
         * Create a new processor.
         *
         * @param statsFilter      stats filter for the query to be processed
         * @param recordsProcessor records-processing utility suited to the underlying records
         * @return a new processor instance
         */
        ComputedPropertiesProcessor getProcessor(
                StatsFilter statsFilter, RecordsProcessor recordsProcessor);
    }

}
