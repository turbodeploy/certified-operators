/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.jooq.Field;
import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;

/**
 * {@link AbstractRecordsAggregator} provides a common behavior for {@link RecordsAggregator}
 * implementations.
 *
 * @param <R> type of the record that should be aggregated.
 */
public abstract class AbstractRecordsAggregator<R extends Record> implements RecordsAggregator<R> {
    /**
     * Random stats table. All stats tables has the same structure
     */
    protected static final VmStatsLatest STATS_TABLE = VmStatsLatest.VM_STATS_LATEST;
    private static final String GROUP_BY_KEY_DELIMITER = "-";
    // map from groupBy string provided by API to the corresponding table field name in DB
    // virtualDisk id is saved as commodity key in db, thus mapped to commodity key column
    private final Map<String, Field<?>> apiGroupByToTableField;

    /**
     * Creates {@link AbstractRecordsAggregator} instance.
     *
     * @param apiGroupByToTableField table specific mapping from group by to table field.
     */
    protected AbstractRecordsAggregator(Map<String, Field<?>> apiGroupByToTableField) {
        final Builder<String, Field<?>> builder = ImmutableMap.builder();
        builder.putAll(apiGroupByToTableField);
        builder.put(StringConstants.KEY, STATS_TABLE.COMMODITY_KEY);
        builder.put(StringConstants.VIRTUAL_DISK, STATS_TABLE.COMMODITY_KEY);
        this.apiGroupByToTableField = builder.build();
    }

    /**
     * Creates record key, based on the requests from API.
     *
     * @param record for which we need to create a key.
     * @param commodityRequests commodity requests that defining the way how we
     *                 should group records, i.e. how record key should look like.
     * @return record key that will be used for records grouping process.
     */
    @Nonnull
    protected String createRecordKey(@Nonnull R record,
                    @Nonnull Collection<CommodityRequest> commodityRequests) {
        // Need to separate commodity bought and sold as some commodities are both bought
        // and sold in the same entity, e.g., StorageAccess in Storage entity.

        final String commodityName = getPropertyType(record);
        final StringBuilder recordKeyBuilder = new StringBuilder(commodityName);

        // See the enum RelationType in com.vmturbo.history.db.
        // Commodities, CommoditiesBought, and CommoditiesFromAttributes
        // (e.g., priceIndex, numVCPUs, etc.)
        final String relation = getRelation(record);
        addItemToRecordKey(recordKeyBuilder, relation);
        final String groupByKey = createGroupByKey(record, commodityRequests, commodityName);
        addItemToRecordKey(recordKeyBuilder, groupByKey);
        // Need to separate commodities by entity_type if the field exists, as some
        // commodities (e.g. CPUAllocation) are sold by more than one entity type
        // (VirtualDataCenter and PhysicalMachine)
        if (record.field(StringConstants.ENTITY_TYPE) != null) {
            addItemToRecordKey(recordKeyBuilder,
                            record.getValue(StringConstants.ENTITY_TYPE, String.class));
        }
        return recordKeyBuilder.toString();
    }

    private static void addItemToRecordKey(@Nonnull StringBuilder recordKeyBuilder,
                    @Nonnull String item) {
        recordKeyBuilder.append(GROUP_BY_KEY_DELIMITER);
        recordKeyBuilder.append(item);
    }

    @Nonnull
    private String createGroupByKey(@Nonnull R record,
                    @Nonnull Collection<CommodityRequest> commodityRequests, String commodityName) {
        // collect groupBy fields for different commodity type and convert to table field names
        final Map<String, Set<String>> commodityNameToGroupByFields =
                        commodityRequests.stream().filter(CommodityRequest::hasCommodityName)
                                        .collect(Collectors
                                                        .toMap(CommodityRequest::getCommodityName,
                                                                        this::getGroupByFieldNames));
        return commodityNameToGroupByFields.getOrDefault(commodityName, Collections.emptySet())
                        .stream().filter(fieldName -> record.field(fieldName) != null)
                        .map(fieldName -> record.getValue(fieldName, String.class))
                        .collect(Collectors.joining(GROUP_BY_KEY_DELIMITER));
    }

    @Nonnull
    private Set<String> getGroupByFieldNames(@Nonnull CommodityRequest request) {
        return request.getGroupByList().stream().map(apiGroupByToTableField::get)
                        .filter(Objects::nonNull).map(Field::getName).collect(Collectors.toSet());
    }

    /**
     * Returns relation type literal from DB record.
     *
     * @param record which relation type we want to know.
     * @return relation type literal of the record.
     */
    @Nonnull
    protected abstract String getRelation(@Nonnull R record);

    /**
     * Returns property type of the DB record.
     *
     * @param record which property type we want to know.
     * @return property type of the DB record.
     */
    @Nonnull
    protected abstract String getPropertyType(@Nonnull R record);
}
